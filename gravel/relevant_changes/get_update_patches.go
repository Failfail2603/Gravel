package relevant_changes

import (
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// func getUpdateChanged(watchQuery *types.WatchQuery, change *types.DBChangeStreamEvent): string {

// an update will change a field in one document

// projected && !filtered && !sorted -> simple replace on field
// if the update is on a projected field but not on a !sorted or !filtered field we can give back a simple replace update to the field

// after this point the projection does not matter anymore as all filtered or sorted fields are automatically projected

// filtered && !sorted ->
// - does the update fall under the filter (check ImDB)
// 		- true: check if document is in window
//			- true: simple replace on field (as there is no sorting the window wont change)
//			- false: (ignore, the update will not change any window as it is not sorted and other windows on the same query will no shift)
//		- false: check if document is in window
//			- true: remove it and shift window -> query for new one and insert at the bottom
//			- false: check if changed document is above window (via sorted fields)
//				- true: the document will be removed from the above window so we need to shift one up (delete first, insert new one below)
//				- false: ignore (the update is sort technical under the window and would not change the documents in the current window)

// sorted && !filtered ->
// - check if the document is in window
// 		- true: check if the update would fall below the window
// 			- true: remove it and shift window -> query for new one and insert at the bottom
// 			- false: check if the update would fall above the window
// 					- true: remove the document and shift window down -> query for new one above and insert at top
//					- false: (update is in window) remove it and get the inserted position via binary search then insert again so we only reorder the list
//		- false: check if the update was made is above the own window
//			-true: requery the first document in window and check the index if
//				- 0: nothing changed -> ignore it
//				- 1: sort changed so the document with the change went from below to above -> shift the window one down
//			-false: check if the update is below the own window
// 				-true: requery the last document in window and check the index if
//					- length - 1: nothing changed -> ignore it
//					- length - 2: the last item went one up so the change made the changed document go from above to below -> shift the window
//				-false: (document not in window, but change will add it to window) -> remove last one and binary search position
//
// sorted && filtered -> (ahhhhh why???!)
// - hybrid from both above
// - update falls under the filter -> all filter check but then do sort checks
// - update does not fall under the filter -> all filter checks for removal and on stay user sort checks
// }

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
		patches := []json_patch.JSONPatch{}

		wasExhaustedWindow := watchQuery.IsExhaustedWindow()

		// add a patch which removes the document from the window and remove the index from the watched document ids
		patches = append(patches, GetSimpleRemovePatch(documentIndex))
		watchQuery.SaveRemoveDocumentFromWindow(documentIndex)

		// check if query window is exhausted. if yes we do not need to query for a new document as there cannot be any new document
		if wasExhaustedWindow {
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

	// updated document is not in window and does not match the filter
	// we need to check if the document previously was above the window
	// if yes we need to shift the window up
	if !matched && !isDocumentInWindow {
		// as this needs a database call we do prechecks to see if the document could even possibly be above the window before the change
		// check if we could even shift the window before checking if we need to. If not we can ignore the update
		if !isWindowShiftApplicable(watchQuery, ShiftUp) {
			return []json_patch.JSONPatch{}
		}

		// if a window shift up is possible we need to check if the document was above the window
		// we do this by querying the first document in the window and checking the index
		firstDocuments := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowStart)

		// check if a document was found
		if len(firstDocuments) == 0 {
			log.Printf("Tried to get the first document in the window but no document was found!")
			return []json_patch.JSONPatch{}
		}

		// if the first document is the same document on index 0 we do not have a change above the window
		if dbService.Connection.GetDocumentID(firstDocuments[0]) == watchQuery.WatchedDocuments[0].ID {
			return []json_patch.JSONPatch{}
		}

		// if the first document of the window is not the same anymore we need to shift the window down so we still do the correct skip
		return ShiftWindow(dbService, watchQuery, ShiftDown)
	}

	return []json_patch.JSONPatch{}

}

func getSimpleSortedUpdatePatch(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, update *types.FieldUpdate, isDocumentInWindow bool, documentIndex int) []json_patch.JSONPatch {
	doc := change.FullDocument

	// retrieve the document info so we can see the sorted fields
	documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(doc.(primitive.M)), watchQuery.QueryInformation)
	if err != nil {
		log.Printf("Error getting watched document info: %v", err)
		return []json_patch.JSONPatch{}
	}

	if isDocumentInWindow {

		positionRelativeToFirst := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

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

	}

	return []json_patch.JSONPatch{}
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

	updatedDocumentIsInWindow, documentIndex := watchQuery.DocumentIsInWindow(change.ID)

	for _, update := range change.Updates {
		isProjectedField := IsProjectedField(watchQuery, &update)
		isFilteredField := IsFilteredField(watchQuery, &update)
		isSortedField := IsSortedField(watchQuery, &update)

		fmt.Printf("IsProjectedField: %v\n", isProjectedField)
		fmt.Printf("IsFilteredField: %v\n", isFilteredField)
		fmt.Printf("IsSortedField: %v\n", isSortedField)

		// check if the field is projected but not filtered or sorted
		// in this case we can simply check if the field is in the window and as nothing to this will change we can give back a simple update patch
		if isProjectedField && !isFilteredField && !isSortedField && updatedDocumentIsInWindow {
			patches = append(patches, getSimpleUpdatePatch(&update, documentIndex))
		} else if isFilteredField && !isSortedField {
			patches = append(patches, getSimpleFilteredUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
		} else if !isFilteredField && isSortedField {
			patches = append(patches, getSimpleSortedUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
		} else {
			log.Printf("Not implemented for both filtered and sorted")
		}

	}

	// as there can be a multiple of updates in one change we need to check if one patch makes other patches useless
	// e.g. There can be a multiple of simple patches and one which removes the document from the window if this is the case we can remove all simple patches
	patches = optimizePatches(patches)

	return patches
}

// #region old code

// func isFieldRelevant(watchQuery *types.WatchQuery, change *types.DBChangeStreamEvent) (bool, bool) {

// 	// if the projection is empty we need to watch the entire document so every change is relevant
// 	if len(watchQuery.QueryInformation.ProjectionFields) == 0 {
// 		log.Println("Projection is empty. Watching entire document. Every change is relevant")
// 		return true, true
// 	}

// 	// relevant fields are all fields that are projected, sorted and filtered. If any of these have changed we know the change is at least relevant to the watchquery
// 	relevantFields := watchQuery.QueryInformation.ProjectionFields
// 	relevantFields = append(relevantFields, watchQuery.QueryInformation.SortFields...)
// 	relevantFields = append(relevantFields, watchQuery.QueryInformation.FilterFields...)
// 	log.Println("Relevant fields: ", relevantFields)

// 	// The change.Document is already the full MongoDB change event
// 	// We need to properly handle the nested structure
// 	var docMap map[string]interface{}

// 	// First try direct casting (in case it's already a map)
// 	if directMap, ok := change.Document.(map[string]interface{}); ok {
// 		docMap = directMap
// 	} else {
// 		// If direct casting fails, marshal and unmarshal to handle nested structures
// 		docBytes, err := json.Marshal(change.Document)
// 		if err != nil {
// 			log.Println("Can't marshal document. Assuming relevant")
// 			return true, true
// 		}

// 		if err := json.Unmarshal(docBytes, &docMap); err != nil {
// 			log.Println("Can't unmarshal document. Assuming relevant")
// 			return true, true
// 		}
// 	}

// 	sortingFields := watchQuery.QueryInformation.SortFields

// 	return false, false
// }

// func isDocumentRelevant(watchQuery *types.WatchQuery, change *types.DBChangeStreamEvent) bool {

// 	// TODO make this better at the moment we have no window shifting
// 	// check if the update is relevant to the watched documents
// 	// check the change document id against the currently watched _ids
// 	if len(watchQuery.WatchedDocumentIds) == 0 {
// 		return false
// 	}

// 	for _, watchedID := range watchQuery.WatchedDocumentIds {
// 		if change.ID == watchedID {
// 			return true
// 		}
// 	}

// 	return false
// }

// // check if the update got made on any relevant field
// func isUpdateRelevant(watchQuery *types.WatchQuery, change *types.DBChangeStreamEvent) bool {

// 	isFieldChangedRelevant, isSortRelevant := isFieldRelevant(watchQuery, change)
// 	isDocumentRelevant := isDocumentRelevant(watchQuery, change)

// 	// the base case here is if a relevant field got changed. if not we can completly ignore the update
// 	// after that we check if the document is relevant by looking in our current window.
// 	// if the document is not in our window we need to check if the change was made on a sorted field
// 	return isFieldChangedRelevant && (isDocumentRelevant || isSortRelevant)
// }

// #endregion old code
