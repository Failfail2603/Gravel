package db

import (
	"gravel/json_patch"
	"gravel/types"
	"slices"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WatchQuery struct {
	ClientID   string
	Hash       string
	Collection string
	Query      string
	Options    string

	// server-side deduplication: multiple clients connecting to the same database share watchqueries.
	// this counter tracks how many client connections are using this watchquery for pooling.
	NumberOfConnections int

	// these are some analytical fields which get computed at the register of the watchquery.
	// they hold information which is used later to determine if a change is relevant for the watchquery
	QueryInformation types.QueryAnalysis

	// currently watched documents
	WatchedDocuments []types.WatchedDocument

	// each watchquery has its own channel to receive updates from the dispatcher
	UpdateChannel chan types.DBChangeStreamEvent

	// Track the last ClusterTime to detect when a new batch starts
	// When a new ClusterTime arrives, we reset ProcessedDocumentIDsInBatch
	LastClusterTime *primitive.Timestamp

	// ProcessedDocumentIDsInBatch tracks document IDs processed in the current ClusterTime batch
	// This is critical for insertMany where multiple documents share the same ClusterTime
	// and we need to handle each insert sequentially (e.g., multiple window shifts)
	ProcessedDocumentIDsInBatch []string

	// ShiftsInBatch tracks cumulative window shifts (up or down) in the current ClusterTime batch
	// Used to adjust query indices when multiple shifts occur at the same snapshot point
	// Positive = shifts up, Negative = shifts down
	ShiftsInBatch int

	// Mutex to protect concurrent access to this watchquery during update processing and shutdown
	// This prevents race conditions when the watchquery is being stopped while an update is being processed
	Mutex sync.RWMutex

	// Stopped flag to signal graceful shutdown
	// When true, update processing should stop immediately
	Stopped bool

	// ReadyChan is closed when the initial query result has been sent to the client.
	// The update processing goroutine should wait on this before processing any updates.
	ReadyChan chan struct{}

	// Unbounded update queue: updates are appended here and drained into UpdateChannel
	pendingUpdates []types.DBChangeStreamEvent
	queueMutex     sync.Mutex
	queueNotify    chan struct{} // capacity 1, signals drainer that items are available
	drainDone      chan struct{} // closed when drainer goroutine exits
}

// EnqueueUpdate appends an update to the unbounded queue (never blocks, never drops).
func (w *WatchQuery) EnqueueUpdate(event types.DBChangeStreamEvent) {
	w.queueMutex.Lock()
	w.pendingUpdates = append(w.pendingUpdates, event)
	w.queueMutex.Unlock()
	// non-blocking signal to the drainer
	select {
	case w.queueNotify <- struct{}{}:
	default:
	}
}

// StartDrainer launches a goroutine that drains the pending queue into UpdateChannel.
func (w *WatchQuery) StartDrainer() {
	w.queueNotify = make(chan struct{}, 1)
	w.drainDone = make(chan struct{})
	go func() {
		defer close(w.drainDone)
		for range w.queueNotify {
			w.drainPending()
		}
		// Final drain after queueNotify is closed to flush remaining items
		w.drainPending()
		close(w.UpdateChannel)
	}()
}

// drainPending sends all queued updates into UpdateChannel.
func (w *WatchQuery) drainPending() {
	for {
		w.queueMutex.Lock()
		if len(w.pendingUpdates) == 0 {
			w.queueMutex.Unlock()
			return
		}
		// Swap out the slice so we can release the lock quickly
		batch := w.pendingUpdates
		w.pendingUpdates = nil
		w.queueMutex.Unlock()

		for _, event := range batch {
			w.UpdateChannel <- event
		}
	}
}

// StopDrainer signals the drainer to flush remaining items and stop.
// It blocks until the drainer has finished and UpdateChannel is closed.
func (w *WatchQuery) StopDrainer() {
	close(w.queueNotify)
	<-w.drainDone
}

func (w *WatchQuery) IsInfiniteWindow() bool {
	return w.QueryInformation.WindowEnd == 0 && w.QueryInformation.WindowStart == 0
}

func (w *WatchQuery) IsDocumentInWindow(documentId string) (bool, int) {
	for i, watchedDocument := range w.WatchedDocuments {
		if documentId == watchedDocument.ID {
			return true, i
		}
	}
	return false, -1
}

// checks if
func (w *WatchQuery) IsExhaustedWindow() bool {

	// every infinite window is exhausted as every document is already in the window
	if w.IsInfiniteWindow() {
		return true
	}

	// if the window is not infinite it is exhausted if there are less documents in the window than the window limit
	return len(w.WatchedDocuments) < w.QueryInformation.WindowLimit
}

func (w *WatchQuery) SaveRemoveDocumentFromWindow(documentIndex int) {
	// if -1 is passed we remove the last document
	if documentIndex == -1 {
		documentIndex = len(w.WatchedDocuments) - 1
	}

	// Guard against empty slice or invalid index
	if len(w.WatchedDocuments) == 0 || documentIndex < 0 || documentIndex >= len(w.WatchedDocuments) {
		return
	}

	// remove the document from the window
	w.WatchedDocuments = slices.Delete(w.WatchedDocuments, documentIndex, documentIndex+1)
}

func (w *WatchQuery) SaveAddDocumentToWindow(dbService *DBService, document types.Document, documentIndex int) {
	watchedDocument, err := dbService.Connection.GetWatchedDocumentInfo(document, w.QueryInformation)
	if err != nil {
		return
	}

	// end of window is -1, or index is at/beyond current length (append case)
	if documentIndex == -1 || documentIndex >= len(w.WatchedDocuments) {
		w.WatchedDocuments = append(w.WatchedDocuments, watchedDocument)
		return
	}

	w.WatchedDocuments = slices.Insert(w.WatchedDocuments, documentIndex, watchedDocument)
}

func (w *WatchQuery) SaveMoveDocumentInWindow(oldIndex int, newIndex int) {
	// nothing to do if indices are the same
	if oldIndex == newIndex {
		return
	}

	// save the document at the old index
	document := w.WatchedDocuments[oldIndex]

	// remove it from the old position
	w.WatchedDocuments = slices.Delete(w.WatchedDocuments, oldIndex, oldIndex+1)

	// adjust the new index if necessary
	// when removing an element before the target position, all indices shift down by 1
	adjustedNewIndex := newIndex
	if oldIndex < newIndex {
		adjustedNewIndex = newIndex - 1
	}

	// insert at the new position
	w.WatchedDocuments = slices.Insert(w.WatchedDocuments, adjustedNewIndex, document)
}

func (w *WatchQuery) SavePatches(dbService *DBService, patches []json_patch.JSONPatch) {
	for _, patch := range patches {
		switch patch.Op {
		case "add":
			// Extract index from path (e.g., "/result/0" -> 0, "/result/-" -> -1)
			index, _ := parseIndexFromPath(patch.Path)
			// Extract document from patch value
			if doc, ok := patch.Value.(types.Document); ok {
				w.SaveAddDocumentToWindow(dbService, doc, index)
			}
		case "remove":
			// Extract index from path
			index, pathFurtherExtended := parseIndexFromPath(patch.Path)

			// only remove if the remove is set on an index. aka there is nothing behind the index
			if !pathFurtherExtended {
				w.SaveRemoveDocumentFromWindow(index)

			}
		case "move":
			// Extract old index from "from" and new index from "path"
			oldIndex, _ := parseIndexFromPath(patch.From)
			newIndex, _ := parseIndexFromPath(patch.Path)
			w.SaveMoveDocumentInWindow(oldIndex, newIndex)

		case "replace":
			// Check if this is a replace on a sorted field
			index, field := parseIndexAndFieldFromPath(patch.Path)
			if index == -1 {
				continue
			}

			// Check if the field is a sorted field
			isSortedField := false
			sortFieldIndex := -1
			for i, sortField := range w.QueryInformation.SortFields {
				if sortField.Field == field {
					isSortedField = true
					sortFieldIndex = i
					break
				}
			}

			// this case only happens if the sorted field changed but it did not change the sorted order.
			// If it's a sorted field, update the WatchedDocument's sort values
			if isSortedField && index < len(w.WatchedDocuments) {
				w.WatchedDocuments[index].SortValues[sortFieldIndex] = patch.Value
			}
		}
	}
}

// parseIndexFromPath extracts the index from a JSON patch path
// e.g., "/result/0" -> 0, "/result/-" -> -1
func parseIndexFromPath(path string) (int, bool) {
	parts := strings.Split(path, "/")

	// if length is bigger than 3 we have an additional path behind the index
	pathFurtherExtended := len(parts) > 3

	if len(parts) < 3 {
		return -1, false
	}

	if parts[2] == "-" {
		return -1, pathFurtherExtended
	}

	index, err := strconv.Atoi(parts[2])
	if err != nil {
		return -1, pathFurtherExtended
	}

	return index, pathFurtherExtended
}

// parseIndexAndFieldFromPath extracts both the index and field from a JSON patch path
// e.g., "/result/0/name" -> (0, "name"), "/result/5/address" -> (5, "address")
func parseIndexAndFieldFromPath(path string) (int, string) {
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		return -1, ""
	}

	if parts[2] == "-" {
		return -1, ""
	}

	index, err := strconv.Atoi(parts[2])
	if err != nil {
		return -1, ""
	}

	// Join remaining parts with "." for nested fields (e.g., "/result/0/address/city" -> "address.city")
	field := strings.Join(parts[3:], ".")

	return index, field
}
