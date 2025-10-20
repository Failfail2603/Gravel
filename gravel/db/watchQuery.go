package db

import (
	"gravel/types"
	"slices"
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

func (w *WatchQuery) DocumentIsInWindow(documentId string) (bool, int) {
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
