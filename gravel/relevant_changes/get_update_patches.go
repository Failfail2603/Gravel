package relevant_changes

import (
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"log"

	"go.mongodb.org/mongo-driver/bson"
)

// func getUpdateChanged(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent): string {

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

func getSimpleUpdatePatch(update *db.FieldUpdate, documentIndex int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:    "replace",
		Path:  json_patch.GetBasePatchPath(documentIndex) + "/" + update.Field,
		Value: update.Value,
	}
}

func getSimpleFilteredUpdatePatch(dbService *db.DBService, watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent, update *db.FieldUpdate, isDocumentInWindow bool, documentIndex int) []json_patch.JSONPatch {

	doc := change.FullDocument

	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, doc)
	println("Matched: ", matched)
	if err != nil {
		log.Printf("Error testing filter: %v", err)
		return []json_patch.JSONPatch{}
	}

	// filter still matches and object is in window
	if matched && isDocumentInWindow {
		return []json_patch.JSONPatch{getSimpleUpdatePatch(update, documentIndex)}
	}

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
		newDocument := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, watchQuery.QueryInformation.WindowEnd-1)

		// we already checked if the window was exhausted before, but if we are exactly at the end of the window we cannot detemrine exhaustion by length of watched _ids. In this case the newDocument will be nil
		if newDocument == nil {
			return patches
		}

		// Extract document ID using the database provider
		newDocID, err := dbService.Connection.GetIDFromEntry(newDocument)
		if err != nil {
			fmt.Printf("Failed to extract document ID: %v\n", err)
			return patches
		}

		fmt.Printf("New document ID: %s\n", newDocID)

		// Parse document for the patch
		newDocumentParsed, ok := newDocument.(bson.M)
		if !ok {
			fmt.Printf("Failed to assert document type: %v", newDocument)
			return patches
		}

		// add the new document at the end of the array
		patches = append(patches, GetSimpleAddPatch(-1, newDocumentParsed))
		watchQuery.SaveAddDocumentToWindow(newDocID, -1)

		// debug print the tracked ids
		fmt.Printf("Tracked IDs: %v\n", watchQuery.WatchedDocumentIds)

		return patches

	}

	// filter does not longer match

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

func GetUpdatePatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	updatedDocumentIsInWindow, documentIndex := watchQuery.DocumentIsInWindow(change.ID)

	for _, update := range change.Updates {
		isProjectedField := IsProjectedField(watchQuery, &update)
		isFilteredField := IsFilteredField(watchQuery, &update)
		isSortedField := IsSortedField(watchQuery, &update)

		// check if the field is projected but not filtered or sorted
		// in this case we can simply check if the field is in the window and as nothing to this will change we can give back a simple update patch
		if isProjectedField && !isFilteredField && !isSortedField && updatedDocumentIsInWindow {
			patches = append(patches, getSimpleUpdatePatch(&update, documentIndex))
		}

		if isFilteredField && !isSortedField {
			patches = append(patches, getSimpleFilteredUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
		}

	}

	// as there can be a multiple of updates in one change we need to check if one patch makes other patches useless
	// e.g. There can be a multiple of simple patches and one which removes the document from the window if this is the case we can remove all simple patches
	patches = optimizePatches(patches)

	return patches
}

// #region old code

// func isFieldRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) (bool, bool) {

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

// func isDocumentRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

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
// func isUpdateRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

// 	isFieldChangedRelevant, isSortRelevant := isFieldRelevant(watchQuery, change)
// 	isDocumentRelevant := isDocumentRelevant(watchQuery, change)

// 	// the base case here is if a relevant field got changed. if not we can completly ignore the update
// 	// after that we check if the document is relevant by looking in our current window.
// 	// if the document is not in our window we need to check if the change was made on a sorted field
// 	return isFieldChangedRelevant && (isDocumentRelevant || isSortRelevant)
// }

// #endregion old code
