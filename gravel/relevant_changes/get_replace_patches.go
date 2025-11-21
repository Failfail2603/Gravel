package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// GetReplacePatches handles the replace operation which replaces an entire document
// Similar to update but treats the entire document as changed rather than individual fields
func GetReplacePatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	// Check if the replaced document is currently in the window
	replacedDocumentIsInWindow, documentIndex := watchQuery.IsDocumentInWindow(change.ID)

	// Test if the new document matches the filter
	newMatched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocument.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter with new document: %v", err)
		return []json_patch.JSONPatch{}
	}

	// Test if the old document matched the filter
	oldMatched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter with old document: %v", err)
		return []json_patch.JSONPatch{}
	}

	// Case 1: Old didn't match, new doesn't match -> ignore
	if !oldMatched && !newMatched {
		log.Println("Replace: Neither old nor new document matches the query, ignoring")
		return []json_patch.JSONPatch{}
	}

	// Case 2: Old matched, new doesn't match -> treat as removal
	if oldMatched && !newMatched {
		log.Println("Replace: Old matched but new doesn't, treating as removal")

		if replacedDocumentIsInWindow {
			// Document is in window and no longer matches, remove it
			return removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex, change)
		}

		// Document was above the window and no longer matches
		return checkShiftWindowOnDocumentRemovedAboveWindow(dbService, watchQuery, change, false, false)
	}

	// Case 3: Old didn't match, new matches -> treat as insertion
	if !oldMatched && newMatched {
		log.Println("Replace: Old didn't match but new does, treating as insertion")

		// Get the document info for the new document
		documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocument.(primitive.M)), watchQuery.QueryInformation)
		if err != nil {
			log.Printf("Error getting watched document info: %v", err)
			return []json_patch.JSONPatch{}
		}

		// Handle as if it's an insert into the window
		return handleReplaceAsInsert(dbService, watchQuery, change, documentInfo)
	}

	// Case 4: Both old and new match -> need to check sorting and position
	log.Println("Replace: Both old and new documents match, checking if position changed")
	return handleReplaceWithBothMatching(dbService, watchQuery, change, replacedDocumentIsInWindow, documentIndex)
}

// handleReplaceAsInsert handles the case where a replace makes a document match the query for the first time
func handleReplaceAsInsert(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, documentInfo types.WatchedDocument) []json_patch.JSONPatch {

	// If infinite window, always add the document
	if watchQuery.IsInfiniteWindow() {
		newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

		// Get and project the new document
		newDocument, err := dbService.Connection.ProjectDocument(types.Document(change.FullDocument.(primitive.M)), watchQuery.Options, "")
		if err != nil {
			log.Printf("Error projecting document: %v", err)
			return []json_patch.JSONPatch{}
		}

		return []json_patch.JSONPatch{GetSimpleAddPatch(newIndex, newDocument)}
	}

	// Check if document should be above the window
	if watchQuery.QueryInformation.WindowStart != 0 {
		positionRelativeToFirst := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

		if positionRelativeToFirst == 1 {
			log.Println("Replace: New document should be above window, shifting up")
			return ShiftWindow(dbService, watchQuery, ShiftUp, change)
		}
	}

	// Check if document should be below the window
	positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	if positionRelativeToLast == -1 {
		log.Println("Replace: New document should be below window, ignoring")
		return []json_patch.JSONPatch{}
	}

	// Document should be inside the window
	patches := []json_patch.JSONPatch{}
	// Remove last document if window is not exhausted
	if !watchQuery.IsExhaustedWindow() {
		patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
	}
	newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

	// Get the new document at the correct position
	newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart+newIndex)

	// Add the document
	patches = append(patches, GetSimpleAddPatch(newIndex, newDocuments[0]))

	log.Println("Replace: New document added to window")
	return patches
}

// handleReplaceWithBothMatching handles the case where both old and new documents match the query
func handleReplaceWithBothMatching(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, isInWindow bool, documentIndex int) []json_patch.JSONPatch {

	// Get document info for new document
	newDocumentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocument.(primitive.M)), watchQuery.QueryInformation)
	if err != nil {
		log.Printf("Error getting new document info: %v", err)
		return []json_patch.JSONPatch{}
	}

	// If document is in the window
	if isInWindow {
		return handleReplaceInWindow(dbService, watchQuery, change, newDocumentInfo, documentIndex)
	}

	// Document is not in window but both versions match
	return handleReplaceOutsideWindow(dbService, watchQuery, change, newDocumentInfo)
}

// handleReplaceInWindow handles replace when the document is currently in the window
func handleReplaceInWindow(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, newDocumentInfo types.WatchedDocument, documentIndex int) []json_patch.JSONPatch {

	// Check if document should move above the window
	positionRelativeToFirst := dbService.Connection.GetSortingOrder(newDocumentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

	if positionRelativeToFirst == 1 {
		// Document should now be above the window
		newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart)

		// If we get the same document back, it means it's just at the edge
		if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[0].ID {
			log.Println("Replace: Document in window would be above but is at edge, replacing in place")
			return []json_patch.JSONPatch{getReplaceValuePatch(dbService, watchQuery, change, documentIndex)}
		}

		patches := []json_patch.JSONPatch{}
		patches = append(patches, GetSimpleRemovePatch(documentIndex))
		patches = append(patches, GetSimpleAddPatch(0, newDocuments[0]))
		log.Println("Replace: Document moved above window, shifting")
		return patches
	}

	// Check if document should move below the window
	positionRelativeToLast := dbService.Connection.GetSortingOrder(newDocumentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	if positionRelativeToLast == -1 {
		// Document should now be below the window
		newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowEnd-1)

		// If we get the same document back, it means it's just at the edge
		if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1].ID {
			log.Println("Replace: Document in window would be below but is at edge, replacing in place")
			return []json_patch.JSONPatch{getReplaceValuePatch(dbService, watchQuery, change, documentIndex)}
		}

		patches := []json_patch.JSONPatch{}
		patches = append(patches, GetSimpleRemovePatch(documentIndex))
		patches = append(patches, GetSimpleAddPatch(-1, newDocuments[0]))
		log.Println("Replace: Document moved below window, shifting")
		return patches
	}

	// Document stays in window, check if position changed
	watchQuery.WatchedDocuments[documentIndex] = newDocumentInfo
	newIndex := dbService.Connection.GetNewPositionForDocument(watchQuery.WatchedDocuments, documentIndex, watchQuery.QueryInformation.SortFields)

	if newIndex != documentIndex {
		// Document needs to move within the window
		patches := []json_patch.JSONPatch{}
		patches = append(patches, getReplaceValuePatch(dbService, watchQuery, change, documentIndex))
		patches = append(patches, getSimpleMovePatch(documentIndex, newIndex))
		log.Println("Replace: Document stayed in window but moved position")
		return patches
	}

	// Document stays at same position, just replace the value
	log.Println("Replace: Document stayed in window at same position, replacing value")
	return []json_patch.JSONPatch{getReplaceValuePatch(dbService, watchQuery, change, documentIndex)}
}

// handleReplaceOutsideWindow handles replace when the document is outside the window
func handleReplaceOutsideWindow(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, newDocumentInfo types.WatchedDocument) []json_patch.JSONPatch {

	// Check if document should move into window from above or below
	positionRelativeToFirst := dbService.Connection.GetSortingOrder(newDocumentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

	// Document is above the window
	if positionRelativeToFirst == 1 {
		// Check if it was above before
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
		if err != nil {
			log.Printf("Error getting old position: %v", err)
			return []json_patch.JSONPatch{}
		}

		// Still above, ignore
		if beforePositionRelativeToFirst == 1 {
			log.Println("Replace: Document was and still is above window, ignoring")
			return []json_patch.JSONPatch{}
		}

		// Moved from below/inside to above, shift window up
		log.Println("Replace: Document moved from below to above window, shifting up")
		return ShiftWindow(dbService, watchQuery, ShiftUp, change)
	}

	// Check if document is below the window
	positionRelativeToLast := dbService.Connection.GetSortingOrder(newDocumentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	if positionRelativeToLast == -1 {
		// Check if it was below before
		beforePositionRelativeToLast, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, -1, Old)
		if err != nil {
			log.Printf("Error getting old position: %v", err)
			return []json_patch.JSONPatch{}
		}

		// Still below, ignore
		if beforePositionRelativeToLast == -1 {
			log.Println("Replace: Document was and still is below window, ignoring")
			return []json_patch.JSONPatch{}
		}

		// Moved from above/inside to below, shift window down
		log.Println("Replace: Document moved from above to below window, shifting down")
		return ShiftWindow(dbService, watchQuery, ShiftDown, change)
	}

	// Document should be inside the window now
	patches := []json_patch.JSONPatch{}

	// Determine where it came from to know what to remove
	insertOffset := 0
	if !watchQuery.IsInfiniteWindow() && watchQuery.QueryInformation.WindowStart > 0 {
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
		if err != nil {
			log.Printf("Error getting old position: %v", err)
			return []json_patch.JSONPatch{}
		}

		if beforePositionRelativeToFirst == 1 {
			// Came from above, remove first document
			patches = append(patches, GetSimpleRemovePatch(0))
			insertOffset = -1
			log.Println("Replace: Document came from above window, removing first")
		} else {
			// Came from below, remove last document
			patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
			log.Println("Replace: Document came from below window, removing last")
		}
	}

	// Add the document at the correct position
	newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, newDocumentInfo, watchQuery.QueryInformation.SortFields)
	newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart+newIndex+insertOffset)
	patches = append(patches, GetSimpleAddPatch(newIndex+insertOffset, newDocuments[0]))

	log.Println("Replace: Document moved into window")
	return patches
}

// getReplaceValuePatch creates a patch that replaces the entire document value at the given index
func getReplaceValuePatch(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, documentIndex int) json_patch.JSONPatch {
	// Project the new document according to the watch query projection
	newDocument, err := dbService.Connection.ProjectDocument(types.Document(change.FullDocument.(primitive.M)), watchQuery.Options, "")
	if err != nil {
		log.Printf("Error projecting document: %v", err)
		// Return an empty patch on error
		return json_patch.JSONPatch{}
	}

	return json_patch.JSONPatch{
		Op:    "replace",
		Path:  json_patch.GetBasePatchPath(documentIndex),
		Value: newDocument,
	}
}
