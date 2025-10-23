package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func getSimpleUpdatePatch(update *types.FieldUpdate, documentIndex int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:    "replace",
		Path:  json_patch.GetBasePatchPath(documentIndex) + "/" + update.Field,
		Value: update.Value,
	}
}

func getSimpleMovePatch(documentIndex int, newPosition int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:   "move",
		From: json_patch.GetBasePatchPath(documentIndex),
		Path: json_patch.GetBasePatchPath(newPosition),
	}
}

func removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService *db.DBService, watchQuery *db.WatchQuery, documentIndex int) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}
	wasExhaustedWindow := watchQuery.IsExhaustedWindow()

	// add a patch which removes the document from the window and remove the index from the watched document ids
	patches = append(patches, GetSimpleRemovePatch(documentIndex))
	watchQuery.SaveRemoveDocumentFromWindow(documentIndex)

	// check if query window is exhausted. if yes we do not need to query for a new document as there cannot be any new document
	// also if we have an infinite window we do not need to query for a new document as there cannot be any new document and all documents should already be inside the window
	if wasExhaustedWindow || watchQuery.IsInfiniteWindow() {
		return patches
	}

	// add a patch which adds the document to the window

	// as the document would fall out of the query we can simply query for the new end of the window
	// as window end is pointing one higher than the last document we need to query the document before
	newDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowEnd-1)

	// we already checked if the window was exhausted before, but if we are exactly at the end of the window we cannot detemrine exhaustion by length of watched _ids. In this case the newDocument will be nil
	if len(newDocuments) == 0 {
		return patches
	}

	// add the new document at the end of the array
	patches = append(patches, GetSimpleAddPatch(-1, newDocuments[0]))
	watchQuery.SaveAddDocumentToWindow(dbService, newDocuments[0], -1)

	return patches
}

func checkShiftWindowOnDocumentRemovedAboveWindow(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, updateMatched bool, updateInWindow bool) []json_patch.JSONPatch {
	// document does not match anymore and was not in window and we are also not at the start of the operation
	if !updateMatched && !updateInWindow && isWindowShiftApplicable(watchQuery, ShiftUp) {

		// check if old document was in the cursor
		oldMatched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
		if err != nil {
			log.Printf("Error testing filter: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the new one did not match and the old one also did not match we can disregard it
		if !oldMatched {
			return []json_patch.JSONPatch{}
		}

		// here we know that we have documents above the window
		// get the old relative position to the first document in the window
		beforePositionRelativeToFirst, err := getPositionOfOldDocumentRelativeTo(dbService, watchQuery, change, 0)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		//if the update is below the first one we are sure it is also below the window in general as an earlier case handled it being inside the window
		if beforePositionRelativeToFirst == -1 {
			return []json_patch.JSONPatch{}
		}

		// at this point we know that an document does not match anymore
		patches := ShiftWindow(dbService, watchQuery, ShiftDown)
		return patches
	}

	return []json_patch.JSONPatch{}
}

func getSimpleFilteredUpdatePatch(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, update *types.FieldUpdate, isDocumentInWindow bool, documentIndex int) []json_patch.JSONPatch {

	doc := change.FullDocument

	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(doc.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter: %v", err)
		return []json_patch.JSONPatch{}
	}

	// filter still matches and object is in window
	if matched && isDocumentInWindow {
		return []json_patch.JSONPatch{getSimpleUpdatePatch(update, documentIndex)}
	}

	// filter does not longer match
	// updated document is not in window anymore
	// we need to remove it and add a new document if there is one
	if !matched && isDocumentInWindow {
		return removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex)
	}

	// updated document matches now is not in window and needs to be inserted
	// get the positions for an insert
	// update is inside the window
	// get where the document should be inserted
	if matched && !isDocumentInWindow {

		// retrieve the document info so we can see the sorted fields
		documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(doc.(primitive.M)), watchQuery.QueryInformation)
		if err != nil {
			log.Printf("Error getting watched document info: %v", err)
			return []json_patch.JSONPatch{}
		}

		newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

		// get the document
		newDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowStart+newIndex)

		patches := []json_patch.JSONPatch{}
		// add the document at the correct position inside the window
		patches = append(patches, GetSimpleAddPatch(newIndex, newDocuments[0]))
		watchQuery.SaveAddDocumentToWindow(dbService, newDocuments[0], newIndex)

		// remove the last document from the window if the query is limited
		if watchQuery.IsInfiniteWindow() {
			return patches
		}

		patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
		watchQuery.SaveRemoveDocumentFromWindow(len(watchQuery.WatchedDocuments) - 1)

		return patches
	}

	// updated document is not in window and does not match the filter
	// we need to check if the document previously was above the window
	// if yes we need to shift the window up
	return checkShiftWindowOnDocumentRemovedAboveWindow(dbService, watchQuery, change, matched, isDocumentInWindow)

}

func getPositionOfOldDocumentRelativeTo(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, relativeTo int) (int, error) {
	beforeDocumentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocumentBeforeChange.(primitive.M)), watchQuery.QueryInformation)
	if err != nil {
		log.Printf("Error getting watched document info: %v", err)
		return -1, err
	}

	// was the document before the change above the window?
	beforePositionRelativeToFirst := dbService.Connection.GetSortingOrder(beforeDocumentInfo, watchQuery.WatchedDocuments[relativeTo], watchQuery.QueryInformation)

	return beforePositionRelativeToFirst, nil
}

func getSimpleSortedUpdatePatch(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, update *types.FieldUpdate, isDocumentInWindow bool, documentIndex int) []json_patch.JSONPatch {
	doc := change.FullDocument

	// retrieve the document info so we can see the sorted fields
	documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(doc.(primitive.M)), watchQuery.QueryInformation)
	if err != nil {
		log.Printf("Error getting watched document info: %v", err)
		return []json_patch.JSONPatch{}
	}

	// where was the update made relative to the first document in the window
	positionRelativeToFirst := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

	if isDocumentInWindow {

		// is now above window
		if positionRelativeToFirst == 1 {
			// remove document from window
			patches := []json_patch.JSONPatch{}
			patches = append(patches, GetSimpleRemovePatch(documentIndex))
			watchQuery.SaveRemoveDocumentFromWindow(documentIndex)

			// query new document
			newDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowStart)

			// we could get the same document as before in this case we return no patches, as the value would be above the old window but not above the next position
			if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[0].ID {
				return []json_patch.JSONPatch{getSimpleUpdatePatch(update, documentIndex)}
			}

			// add document to window
			patches = append(patches, GetSimpleAddPatch(0, newDocuments[0]))
			watchQuery.SaveAddDocumentToWindow(dbService, newDocuments[0], 0)

			return patches

		}

		positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

		// is now below window
		if positionRelativeToLast == -1 {
			// remove document from window
			patches := []json_patch.JSONPatch{}
			patches = append(patches, GetSimpleRemovePatch(documentIndex))
			watchQuery.SaveRemoveDocumentFromWindow(documentIndex)

			// query new document at the end
			newDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowEnd-1)

			// we could get the same document as before in this case we return a simple update patch, as the value would be below the old window but not below the next position
			if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1].ID {
				return []json_patch.JSONPatch{getSimpleUpdatePatch(update, documentIndex)}
			}

			// add document to window
			patches = append(patches, GetSimpleAddPatch(-1, newDocuments[0]))
			watchQuery.SaveAddDocumentToWindow(dbService, newDocuments[0], -1)

			return patches

		}

		// before we can compare the document inside the array we need to update its representation of sorting value as these changed. (else we would not be here)
		watchQuery.WatchedDocuments[documentIndex] = documentInfo

		// is in window and should still be in window. We need check wheter the document is still in the same place
		// search for the correct
		newIndex := dbService.Connection.GetNewPositionForDocument(watchQuery.WatchedDocuments, documentIndex, watchQuery.QueryInformation.SortFields)

		if newIndex != documentIndex {
			// move document
			patches := []json_patch.JSONPatch{}
			patches = append(patches, getSimpleMovePatch(documentIndex, newIndex))
			patches = append(patches, getSimpleUpdatePatch(update, newIndex))
			watchQuery.SaveMoveDocumentInWindow(documentIndex, newIndex)
			return patches
		}

		// if we do not move the document we can just make a simple update statement for the value
		return []json_patch.JSONPatch{getSimpleUpdatePatch(update, documentIndex)}

	}

	// update is outside of window and above the window
	if positionRelativeToFirst == 1 {

		// was the document before the change above the window?
		beforePositionRelativeToFirst, err := getPositionOfOldDocumentRelativeTo(dbService, watchQuery, change, 0)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update is above and the document was also above the window we can ignore it
		if beforePositionRelativeToFirst == 1 {
			return []json_patch.JSONPatch{}
		}

		// at this point we know that the document moved around the window from below to up so we need to shift the window up
		patches := ShiftWindow(dbService, watchQuery, ShiftUp)
		return patches
	}

	positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	// update is outside of window and below the window
	if positionRelativeToLast == -1 {
		// we need to check if the update moved a document below the window
		// retrieve the document info so we can see the sorted fields
		beforePositionRelativeToLast, err := getPositionOfOldDocumentRelativeTo(dbService, watchQuery, change, len(watchQuery.WatchedDocuments)-1)
		if err != nil {
			log.Printf("Error getting position of old document relative to last: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update is above and the document was also above the window we can ignore it
		if beforePositionRelativeToLast == -1 {
			return []json_patch.JSONPatch{}
		}

		// at this point we know that the document moved around the window from above to down so we need to shift the window down
		patches := ShiftWindow(dbService, watchQuery, ShiftDown)
		return patches
	}

	// update moves document inside the window
	// we need to check from where the update is coming
	patches := []json_patch.JSONPatch{}

	// was the original document above the window
	if !watchQuery.IsInfiniteWindow() && watchQuery.QueryInformation.WindowStart > 0 {
		beforePositionRelativeToFirst, err := getPositionOfOldDocumentRelativeTo(dbService, watchQuery, change, 0)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// yes document comes from above window to inside the window
		// in this case we do not delete the last one from our window but instead the first one as it should move up the curso
		if beforePositionRelativeToFirst == 1 {
			patches = append(patches, GetSimpleRemovePatch(0))
			watchQuery.SaveRemoveDocumentFromWindow(0)
		} else {
			// in this case the document was below the window and comes from there. here we should delete the last one from the window
			patches = append(patches, GetSimpleRemovePatch(-1))
			watchQuery.SaveRemoveDocumentFromWindow(-1)
		}
	}

	// get where the document should be inserted
	newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

	// get the document
	newDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowStart+newIndex)

	// add the document at the correct position inside the window
	patches = append(patches, GetSimpleAddPatch(newIndex, newDocuments[0]))
	watchQuery.SaveAddDocumentToWindow(dbService, newDocuments[0], newIndex)

	return patches
}

func getFilteredAndSortedUpdatedPatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, update *types.FieldUpdate, updatedDocumentIsInWindow bool, documentIndex int) []json_patch.JSONPatch {

	doc := change.FullDocument

	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(doc.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter: %v", err)
		return []json_patch.JSONPatch{}
	}

	// matched
	if matched {
		return getSimpleSortedUpdatePatch(dbService, watchQuery, change, update, updatedDocumentIsInWindow, documentIndex)
	}

	// document does not match anymore and was in window -> remove and get new one
	if !matched && updatedDocumentIsInWindow {
		return removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex)
	}

	// if the document did not match and is not inside the window we can try a shift down if the document
	// the function checks this internalls
	return checkShiftWindowOnDocumentRemovedAboveWindow(dbService, watchQuery, change, matched, updatedDocumentIsInWindow)
}

// optimizePatches removes redundant patches from the list
// e.g., if a document is being removed, any replace operations on that document's fields are unnecessary
func optimizePatches(patches []json_patch.JSONPatch) []json_patch.JSONPatch {
	// Collect all base paths that are being removed
	removedPaths := make(map[string]bool)
	for _, patch := range patches {
		if patch.Op == "remove" {
			removedPaths[patch.Path] = true
		}
	}

	// If no removes, return original patches
	if len(removedPaths) == 0 {
		return patches
	}

	// Filter out replace operations on removed documents
	optimized := []json_patch.JSONPatch{}
	for _, patch := range patches {
		shouldKeep := true

		// Check if this is a replace operation on a field within a removed document
		if patch.Op == "replace" {
			for removedPath := range removedPaths {
				// Check if the patch path starts with the removed path + "/"
				// e.g., removedPath="result/0" and patch.Path="result/0/name"
				if len(patch.Path) > len(removedPath) &&
					patch.Path[:len(removedPath)] == removedPath &&
					patch.Path[len(removedPath)] == '/' {
					shouldKeep = false
					break
				}
			}
		}

		if shouldKeep {
			optimized = append(optimized, patch)
		}
	}

	return optimized
}

func GetUpdatePatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	updatedDocumentIsInWindow, documentIndex := watchQuery.IsDocumentInWindow(change.ID)

	for _, update := range change.Updates {
		isProjectedField := IsProjectedField(watchQuery, &update)
		isFilteredField := IsFilteredField(watchQuery, &update)
		isSortedField := IsSortedField(watchQuery, &update)

		// check if the field is projected but not filtered or sorted
		// in this case we can simply check if the field is in the window and as nothing to this will change we can give back a simple update patch
		if isProjectedField && !isFilteredField && !isSortedField && updatedDocumentIsInWindow {
			patches = []json_patch.JSONPatch{getSimpleUpdatePatch(&update, documentIndex)}
		} else if isFilteredField && !isSortedField {
			patches = getSimpleFilteredUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)
		} else if !isFilteredField && isSortedField {
			patches = getSimpleSortedUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)
		} else if isFilteredField && isSortedField {
			patches = getFilteredAndSortedUpdatedPatches(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)
		} else {
			// ignore everything else
			continue
		}

	}

	// as there can be a multiple of updates in one change we need to check if one patch makes other patches useless
	// e.g. There can be a multiple of simple patches and one which removes the document from the window if this is the case we can remove all simple patches
	// TODO much todo here as we probably also need to check some update orders
	patches = optimizePatches(patches)

	return patches
}
