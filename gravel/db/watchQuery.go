package db

import (
	"gravel/json_patch"
	"gravel/types"
	"slices"
	"strconv"
	"strings"
)

type WatchQuery struct {
	ClientID   string
	Hash       string
	Collection string
	Query      string
	Options    string

	// we dedube watchqueries by hash, so we need to count the number of connections to the same watchquery
	// the multiplexing to the different queries observables will be handled by the client
	NumberOfConnections int

	// these are some analytical fields which get computed at the register of the watchquery.
	// they hold information which is used later to determine if a change is relevant for the watchquery
	QueryInformation types.QueryAnalysis

	// currently watched documents
	WatchedDocuments []types.WatchedDocument
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

	// remove the document from the window
	w.WatchedDocuments = slices.Delete(w.WatchedDocuments, documentIndex, documentIndex+1)
}

func (w *WatchQuery) SaveAddDocumentToWindow(dbService *DBService, document types.Document, documentIndex int) {
	watchedDocument, err := dbService.Connection.GetWatchedDocumentInfo(document, w.QueryInformation)
	if err != nil {
		return
	}

	// end of window is -1
	if documentIndex == -1 {
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
			index := parseIndexFromPath(patch.Path)
			// Extract document from patch value
			if doc, ok := patch.Value.(types.Document); ok {
				w.SaveAddDocumentToWindow(dbService, doc, index)
			}
		case "remove":
			// Extract index from path
			index := parseIndexFromPath(patch.Path)
			w.SaveRemoveDocumentFromWindow(index)
		case "move":
			// Extract old index from "from" and new index from "path"
			oldIndex := parseIndexFromPath(patch.From)
			newIndex := parseIndexFromPath(patch.Path)
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

			// If it's a sorted field, update the WatchedDocument's sort values
			if isSortedField && index < len(w.WatchedDocuments) {
				w.WatchedDocuments[index].SortValues[sortFieldIndex] = patch.Value
			}
		}
	}
}

// parseIndexFromPath extracts the index from a JSON patch path
// e.g., "/result/0" -> 0, "/result/-" -> -1
func parseIndexFromPath(path string) int {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return -1
	}

	if parts[2] == "-" {
		return -1
	}

	index, err := strconv.Atoi(parts[2])
	if err != nil {
		return -1
	}

	return index
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
