package relevant_changes

import (
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func getSimpleUpdatePatch(dbService *db.DBService, watchQuery *db.WatchQuery, update *types.FieldUpdate, documentIndex int) json_patch.JSONPatch {
	// as the path is dot separated we need to replace the dots with slashes
	path := update.Field
	for strings.Contains(path, ".") {
		path = strings.Replace(path, ".", "/", -1)
	}

	// Handle unset operations - create a remove patch
	if update.Operation == "unset" {
		return json_patch.JSONPatch{
			Op:   "remove",
			Path: json_patch.GetBasePatchPath(documentIndex) + "/" + path,
		}
	}

	// Handle set operations - create a replace patch
	// as a value can be a nested object we need to project it according to the projection in the watchquery
	// check if the update value is an objects versus a primitive value
	value := update.Value
	log.Printf("update value: %v", update.Value)
	if mapVal, ok := update.Value.(map[string]interface{}); ok {
		log.Println("found object in patched value")
		value, _ = dbService.Connection.ProjectDocument(types.Document(mapVal), watchQuery.Options, update.Field)
	} else if arrayVal, ok := update.Value.([]interface{}); ok {
		log.Println("found array in patched value. Checking each subvalue for projection")
		// Project each element in the array if it's a document
		projectedArray := make([]interface{}, len(arrayVal))
		for i, elem := range arrayVal {
			if elemMap, ok := elem.(map[string]interface{}); ok {
				projectedDoc, _ := dbService.Connection.ProjectDocument(types.Document(elemMap), watchQuery.Options, update.Field)
				projectedArray[i] = projectedDoc
			} else {
				projectedArray[i] = elem
			}
		}
		value = projectedArray
	} else {
		log.Println("found primitive in patched value")
	}

	return json_patch.JSONPatch{
		Op:    "replace",
		Path:  json_patch.GetBasePatchPath(documentIndex) + "/" + path,
		Value: value,
	}
}

func getSimpleMovePatch(documentIndex int, newPosition int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:   "move",
		From: json_patch.GetBasePatchPath(documentIndex),
		Path: json_patch.GetBasePatchPath(newPosition),
	}
}

func removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService *db.DBService, watchQuery *db.WatchQuery, documentIndex int, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}
	wasExhaustedWindow := watchQuery.IsExhaustedWindow()

	// add a patch which removes the document from the window and remove the index from the watched document ids
	patches = append(patches, GetSimpleRemovePatch(documentIndex))

	// check if query window is exhausted. if yes we do not need to query for a new document as there cannot be any new document
	// also if we have an infinite window we do not need to query for a new document as there cannot be any new document and all documents should already be inside the window
	if wasExhaustedWindow || watchQuery.IsInfiniteWindow() {
		return patches
	}

	// add a patch which adds the document to the window

	// as the document would fall out of the query we can simply query for the new end of the window
	// as window end is pointing one higher than the last document we need to query the document before
	newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowEnd-1)

	// we already checked if the window was exhausted before, but if we are exactly at the end of the window we cannot detemrine exhaustion by length of watched _ids. In this case the newDocument will be nil
	if len(newDocuments) == 0 {
		return patches
	}

	// add the new document at the end of the array
	patches = append(patches, GetSimpleAddPatch(-1, newDocuments[0]))

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
			log.Println("Document was not matched and is not matched, disregarding")
			return []json_patch.JSONPatch{}
		}

		// here we know that we have documents above the window
		// get the old relative position to the first document in the window
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		//if the update is below the first one we are sure it is also below the window in general as an earlier case handled it being inside the window
		if beforePositionRelativeToFirst == -1 {
			log.Println("Document was matched and is now not matched and was below the window, disregarding")
			return []json_patch.JSONPatch{}
		}

		// at this point we know that an document does not match anymore
		log.Println("Document was matched and is now not matched and was above the window, shifting window down")
		patches := ShiftWindow(dbService, watchQuery, ShiftDown, change)
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
		log.Println("Only Filtered, matched and in Window")
		return []json_patch.JSONPatch{getSimpleUpdatePatch(dbService, watchQuery, update, documentIndex)}
	}

	// filter does not longer match
	// updated document is not in window anymore
	// we need to remove it and add a new document if there is one
	if !matched && isDocumentInWindow {
		log.Println("Only Filtered, not matched and in Window")
		return removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex, change)
	}

	// updated document matches now is not in window it could
	// get the positions for an insert
	// update is inside the window
	// get where the document should be inserted
	if matched {

		// at this point we know that we have no infinite window as this case would have branched of early
		// check if old document matched
		beforeMatched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
		if err != nil {
			log.Printf("Error testing filter: %v", err)
			return []json_patch.JSONPatch{}
		}

		//if the old document matched we can ignore the update as no sorting field was updated and the document cannot shift its position
		if beforeMatched {
			log.Println("Only Filtered, matched and was not in Window, outside window")
			return []json_patch.JSONPatch{}
		}

		// from the statements for the window above we know that we have not an infinite window so we need to check if the document should be above the window now

		// retrieve the document info so we can see the sorted fields
		documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(doc.(primitive.M)), watchQuery.QueryInformation)
		if err != nil {
			log.Printf("Error getting watched document info: %v", err)
			return []json_patch.JSONPatch{}
		}

		// check if the position of the updated document should be above the window
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, New)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update would be above the current window we shift one up to keep the correct skip
		if beforePositionRelativeToFirst == 1 {
			log.Println("Document did not match before and is now matched and would be above the window. Shifting window up")
			return ShiftWindow(dbService, watchQuery, ShiftUp, change)
		}

		// if the new document in the cursor is not above the window so check if it is below the window
		beforePositionRelativeToLast, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, len(watchQuery.WatchedDocuments)-1, New)
		if err != nil {
			log.Printf("Error getting position of old document relative to last: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update is below the window we can ignore it
		if beforePositionRelativeToLast == -1 {
			log.Println("Document did not match before and is now matched and would be below the window. Ignoring")
			return []json_patch.JSONPatch{}
		}

		// here we now know that the new document should be inside the window so we try to find the correct position
		// get the new index for the document
		newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

		// get the document
		newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart+newIndex)

		patches := []json_patch.JSONPatch{}
		// add the document at the correct position inside the window
		patches = append(patches, GetSimpleAddPatch(newIndex, newDocuments[0]))

		// remove the last document from the window if the query is limited
		// early return if it is an exhausted window or no limit was specified
		if watchQuery.IsExhaustedWindow() && watchQuery.QueryInformation.WindowLimit == 0 {
			log.Println("Only Filtered, matched and was not in Window, infinite window")
			return patches
		}

		log.Println("Only Filtered, matched and was not in Window, limited window")
		patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))

		return patches
	}

	// updated document is not in window and does not match the filter
	// we need to check if the document previously was above the window
	// if yes we need to shift the window up

	return checkShiftWindowOnDocumentRemovedAboveWindow(dbService, watchQuery, change, matched, isDocumentInWindow)

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

		// document should now be above the window
		if positionRelativeToFirst == 1 {
			// remove document from window

			// query new document
			newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart)

			// we could get the same document as before in this case we return no patches, as the value would be above the old window but not above the next position
			if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[0].ID {
				log.Println("Sorting. Document is in window and should now be above the window. The returned document is the same so we just send an update patch.")
				return []json_patch.JSONPatch{getSimpleUpdatePatch(dbService, watchQuery, update, documentIndex)}
			}

			patches := []json_patch.JSONPatch{}
			patches = append(patches, GetSimpleRemovePatch(documentIndex))

			// add document to window
			patches = append(patches, GetSimpleAddPatch(0, newDocuments[0]))
			log.Println("Sorting. Document was in window and is now above the window. Added new document to the top of the window")
			return patches

		}

		positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

		// is now below window
		if positionRelativeToLast == -1 {
			// remove document from window

			// query new document at the end
			newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowEnd-1)

			// we could get the same document as before in this case we return a simple update patch, as the value would be below the old window but not below the next position
			if dbService.Connection.GetDocumentID(newDocuments[0]) == watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1].ID {
				log.Println("Sorting. Document is in window and should now be below the window. The returned document is the same so we just send an update patch.")
				return []json_patch.JSONPatch{getSimpleUpdatePatch(dbService, watchQuery, update, documentIndex)}
			}

			patches := []json_patch.JSONPatch{}
			patches = append(patches, GetSimpleRemovePatch(documentIndex))

			// add document to window
			patches = append(patches, GetSimpleAddPatch(-1, newDocuments[0]))
			log.Println("Sorting. Document was in window and is now below the window. Added new document to the end of the window")
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
			patches = append(patches, getSimpleUpdatePatch(dbService, watchQuery, update, documentIndex))
			patches = append(patches, getSimpleMovePatch(documentIndex, newIndex))
			log.Println("Sorting. Document was in window and is still in window but moved to a different position. Move the document to the new position. And update the value.")
			return patches
		}

		// if we do not move the document we can just make a simple update statement for the value
		log.Println("Sorting. Document was in window and is still in window but at the same position. Update the value.")
		return []json_patch.JSONPatch{getSimpleUpdatePatch(dbService, watchQuery, update, documentIndex)}

	}

	// update is above the current window and was in cursor
	if positionRelativeToFirst == 1 {

		// was the document before the change above the window?
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update is above and the document was also above the window we can ignore it
		if beforePositionRelativeToFirst == 1 {
			log.Println("Sorting. Old Document matched and was above the window. Still above the window so ignore it.")
			return []json_patch.JSONPatch{}
		}

		// at this point we know that the document moved around the window from below to up so we need to shift the window up
		log.Println("Sorting. Old Document matched and was below the window and is now above the window. Shift the window up.")
		patches := ShiftWindow(dbService, watchQuery, ShiftUp, change)
		return patches
	}

	positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	// update is below the current window and was in cursor
	if positionRelativeToLast == -1 {
		// we need to check if the update moved a document below the window
		// retrieve the document info so we can see the sorted fields
		beforePositionRelativeToLast, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, -1, Old)
		if err != nil {
			log.Printf("Error getting position of old document relative to last: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the update was below and the document was also below the window we can ignore it
		if beforePositionRelativeToLast == -1 {
			log.Println("Sorting. Old Document matched and was below the window. Still below the window so ignore it.")
			return []json_patch.JSONPatch{}
		}

		// at this point we know that the document moved around the window from above to down so we need to shift the window down
		log.Println("Sorting. Old Document matched and was above the window and is now below the window. Shift the window down.")
		patches := ShiftWindow(dbService, watchQuery, ShiftDown, change)
		return patches
	}

	patches := []json_patch.JSONPatch{}

	// if we delete a document from the window at index 0 we later need to insert the one one sport above as the calculation of the insertion is still on the old state
	insertOffset := 0

	if !watchQuery.IsInfiniteWindow() && watchQuery.QueryInformation.WindowStart > 0 {
		beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
		if err != nil {
			log.Printf("Error getting position of old document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// yes document comes from above window to inside the window
		// in this case we do not delete the last one from our window but instead the first one as it should move up the cursor
		if beforePositionRelativeToFirst == 1 {
			patches = append(patches, GetSimpleRemovePatch(0))

			// document was deleted at the start of the window so offset insertion by -1
			insertOffset = -1

			log.Println("Sorting. Old did match and new one also. Update was outside the window and now should be added. Document was above the window. So remove the first one from the window.")
		} else {
			// in this case the document was below the window and comes from there. here we should delete the last one from the window
			patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
			log.Println("Sorting. Old did match and new one also. Update was outside the window and now should be added. Document was below the window. So remove the last one from the window.")
		}
	}

	// get where the document should be inserted
	newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

	// get the document
	newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart+newIndex+insertOffset)

	// add the document at the correct position inside the window
	patches = append(patches, GetSimpleAddPatch(newIndex+insertOffset, newDocuments[0]))
	log.Println("Sorting. Document moved into the window. Add it to the window.")
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
		oldMatched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
		if err != nil {
			log.Printf("Error testing filter: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the old one and the new one matched we just need to adjust the sorting
		if oldMatched {
			return getSimpleSortedUpdatePatch(dbService, watchQuery, change, update, updatedDocumentIsInWindow, documentIndex)
		}

		documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(doc.(primitive.M)), watchQuery.QueryInformation)
		if err != nil {
			log.Printf("Error getting watched document info: %v", err)
			return []json_patch.JSONPatch{}
		}

		// update was outside of the cursor and is now inside
		// we need to determine if it should be inside of the window now

		// if we have not determined if the document should be inside the window by checking if it is infinite we need to check if it is above the window
		// we could be above
		if !watchQuery.IsInfiniteWindow() && watchQuery.QueryInformation.WindowStart > 0 {

			positionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, New)
			if err != nil {
				log.Printf("Error getting position of document relative to index: %v", err)
				return []json_patch.JSONPatch{}
			}
			// update is in cursor but above window
			if positionRelativeToFirst == 1 {
				// shift the window one up to keep skipping documents
				log.Println("Sorting. Old did not match. But new one is. Should be inserted above the window so shift up.")
				return ShiftWindow(dbService, watchQuery, ShiftUp, change)
			}
		}

		// check if window is exhausted. if it is not exhausted and we have a limit we could have the document below the window
		if !watchQuery.IsExhaustedWindow() {

			positionRelativeToLast, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, -1, New)
			if err != nil {
				log.Printf("Error getting position of document relative to index: %v", err)
				return []json_patch.JSONPatch{}
			}

			// update is in cursor but below the window
			if positionRelativeToLast == -1 {
				// we can ignore this case
				log.Println("Sorting. Old did not match. But new one is. Update is below the window so ignore it.")
				return []json_patch.JSONPatch{}
			}
		}

		patches := []json_patch.JSONPatch{}
		// if we get here we know that the document should be now inside the window
		// if we are in a non exhausted window we need to remove the last one to make space
		if !watchQuery.IsExhaustedWindow() {
			patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
		}

		// get where the document should be inserted
		newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

		// query new document
		newDocuments := GetSingleDocumentOnIndex(dbService, watchQuery, change, watchQuery.QueryInformation.WindowStart+newIndex)

		// add the document at the correct position inside the window
		patches = append(patches, GetSimpleAddPatch(newIndex, newDocuments[0]))

		// document was not in cursor but should now be inside cursor and window
		log.Println("Document was previously outside of the cursor and now matches and should also be inside the window")
		return patches
	}

	// document does not match anymore and was in window -> remove and get new one
	if !matched && updatedDocumentIsInWindow {
		return removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex, change)
	}

	// if the document did not match and is not inside the window we can try a shift down if the document
	// the function checks this internalls
	return checkShiftWindowOnDocumentRemovedAboveWindow(dbService, watchQuery, change, matched, updatedDocumentIsInWindow)
}

// extractIDFromValue attempts to extract the _id field from a patch value
// Returns empty string if _id cannot be extracted
func extractIDFromValue(value interface{}) string {
	if value == nil {
		return ""
	}

	// Try to convert to map[string]interface{}
	if mapVal, ok := value.(map[string]interface{}); ok {
		if id, exists := mapVal["_id"]; exists {
			return fmt.Sprint(id)
		}
	}

	// Try to convert to types.Document
	if docVal, ok := value.(types.Document); ok {
		if id, exists := docVal["_id"]; exists {
			return fmt.Sprint(id)
		}
	}

	return ""
}

// optimizePatches removes redundant patches from the list
// e.g., if a document is being removed, any replace operations on that document's fields are unnecessary
// Also filters duplicates and ensures only one move/add/remove patch per document
func optimizePatches(patches []json_patch.JSONPatch) []json_patch.JSONPatch {
	// Collect all base paths that are being removed
	removedPaths := make(map[string]bool)
	for _, patch := range patches {
		if patch.Op == "remove" {
			removedPaths[patch.Path] = true
		}
	}

	// Filter out replace operations on removed documents and track seen patches
	optimized := []json_patch.JSONPatch{}
	seenPatches := make(map[string]json_patch.JSONPatch) // key format: "op:path" or "op:from:path" for move

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

		if !shouldKeep {
			continue
		}

		// For move, add, remove operations: ensure only one per document
		if patch.Op == "move" || patch.Op == "add" || patch.Op == "remove" {
			var key string

			// For "add": key based on document _id (only one add per unique document)
			// For "remove" and "move": key based on path/index (only one operation per index)
			switch patch.Op {
			case "add":
				docID := extractIDFromValue(patch.Value)
				if docID == "" {
					log.Printf("Warning: Cannot extract _id from add patch value, using path as fallback")
					key = patch.Op + ":" + patch.Path
				} else {
					key = patch.Op + ":_id:" + docID
				}
			case "move":
				key = patch.Op + ":" + patch.From + ":" + patch.Path
			default: // remove
				key = patch.Op + ":" + patch.Path
			}

			// Check if we've already seen this patch
			if existingPatch, exists := seenPatches[key]; exists {
				if patch.Op == "add" {
					newID := extractIDFromValue(patch.Value)
					log.Printf("Warning: Duplicate add patch detected for document _id %v (attempting to add at path %s, already added at path %s). Keeping first occurrence.", newID, patch.Path, existingPatch.Path)
				} else {
					log.Printf("Warning: Duplicate %s patch detected for path %s. Keeping first occurrence.", patch.Op, patch.Path)
				}
				continue
			}

			seenPatches[key] = patch
		}

		optimized = append(optimized, patch)
	}

	return optimized
}

func GetUpdatePatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	log.Printf("Full document before change: %v", change.FullDocumentBeforeChange)
	for _, update := range change.Updates {
		isProjectedField := IsProjectedField(watchQuery, &update)
		isFilteredField := IsFilteredField(watchQuery, &update)
		isSortedField := IsSortedField(watchQuery, &update)

		// we need to do this in the loop as the document might be moved with the generation of a patch
		updatedDocumentIsInWindow, documentIndex := watchQuery.IsDocumentInWindow(change.ID)

		log.Printf("Calculating for field %v with new value %v", update.Field, update.Value)

		// check if the field is projected but not filtered or sorted
		// in this case we can simply check if the field is in the window and as nothing to this will change we can give back a simple update patch
		if isProjectedField && !isFilteredField && !isSortedField && updatedDocumentIsInWindow {
			patches = append(patches, getSimpleUpdatePatch(dbService, watchQuery, &update, documentIndex))
			log.Printf("Simple update patch for document %v", documentIndex)
		} else if isFilteredField && !isSortedField {
			patches = append(patches, getSimpleFilteredUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
		} else if !isFilteredField && isSortedField {
			// we only look for sorted updates if the old document even matched the query. As a field changed which is not relevant to the query itself we can disregard everything changed if the query did not match
			matchedBefore, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
			if err != nil {
				log.Printf("Error testing filter: %v", err)
				return []json_patch.JSONPatch{}
			}
			matchedAfter, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocument.(primitive.M)))
			if err != nil {
				log.Printf("Error testing filter: %v", err)
				return []json_patch.JSONPatch{}
			}
			if matchedBefore && matchedAfter {

				patches = append(patches, getSimpleSortedUpdatePatch(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
			} else {
				log.Println("Sorted field changed but document changed state so the filter function will handle it")
			}
		} else if isFilteredField && isSortedField {
			patches = append(patches, getFilteredAndSortedUpdatedPatches(dbService, watchQuery, change, &update, updatedDocumentIsInWindow, documentIndex)...)
		} else {
			// ignore everything else
			log.Printf("Ignoring update for field %v. Update is not relevant. Inside window %v", update.Field, updatedDocumentIsInWindow)
			continue
		}
	}

	// as there can be a multiple of updates in one change we need to check if one patch makes other patches useless
	// e.g. There can be a multiple of simple patches and one which removes the document from the window if this is the case we can remove all simple patches
	// TODO much todo here as we probably also need to check some update orders
	patches = optimizePatches(patches)

	log.Printf("Watchquery watche docs len %v", len(watchQuery.WatchedDocuments))

	return patches
}
