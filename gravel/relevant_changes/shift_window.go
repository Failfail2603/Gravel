package relevant_changes

import (
	"encoding/json"
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
)

// ShiftDirection represents a one-step window shift direction.
type ShiftDirection int

const (
	ShiftUp ShiftDirection = iota
	ShiftDown
)

/**
 * Checks if a one-step window shift is applicable in the given direction.
 * There can be windows which cannot be shifted in the given direction.
 * e.g.: A window at the start or end of the query cannot be shifted further towards the end.
 * e.g.: An unlimited window (no skip or limit) cannot be shifted as it is at limit at both ends
 * @param watchQuery The watchquery to check
 * @param dir The direction to shift (up or down)
 * @return bool True if the change can be made, false if not
 */
func isWindowShiftApplicable(watchQuery *db.WatchQuery, dir ShiftDirection) bool {

	// cannot shift an infinite window
	if watchQuery.IsInfiniteWindow() {
		return false
	}

	// we can shift up if the window start is not 0 (at the start of the cursor) and there are documents
	if dir == ShiftUp && watchQuery.QueryInformation.WindowStart > 0 {
		return true
	}

	// we can shift down if the window end is not at the end of the cursor and there are documents
	// there are two possibilities when a window is at the end of a cursor:
	// 1. we have less documents than the limit -> the window is already exhausted and we cannot shift down
	// 2. we have exactly the limit of documents at the end -> we cannot check this as we do not know if there are documents below
	// this is the only edgecase where this function returns true but the window should not be shiftable. The shift function will need to check for this
	if dir == ShiftDown && watchQuery.QueryInformation.WindowLimit != 0 && watchQuery.QueryInformation.WindowLimit == len(watchQuery.WatchedDocuments) {
		return true
	}

	return false
}

func ShiftWindow(dbService *db.DBService, watchQuery *db.WatchQuery, dir ShiftDirection) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	// early exit if the window is not shiftable
	if !isWindowShiftApplicable(watchQuery, dir) {
		return patches
	}

	// get single document from the cursor in which the window is shifting
	skip := watchQuery.QueryInformation.WindowStart
	if dir == ShiftDown {
		skip = watchQuery.QueryInformation.WindowEnd - 1
	}

	// query the new document
	newDocument := GetSingleDocumentInWindowOnIndex(dbService, watchQuery, skip)

	// Important: If we are at the end of the cursor the query should return an empty array as skip is automatically the number of documents in the system. This is an absolute edgecase as it can only happen if the last window in the cursor is completly full but there are not further documents
	if len(newDocument) == 0 {
		return patches
	}

	// make a patch to delete the document from the window in the end of the shift direction
	removeIndex := 0
	if dir == ShiftUp {
		removeIndex = watchQuery.QueryInformation.WindowLimit - 1
	}

	deletePatch := json_patch.JSONPatch{
		Op:   "remove",
		Path: fmt.Sprintf("/result/%d", removeIndex),
	}
	patches = append(patches, deletePatch)

	// remove the _id from the watched document ids
	watchQuery.SaveRemoveDocumentFromWindow(removeIndex)

	addIndex := "0"
	if dir == ShiftDown {
		addIndex = "-"
	}

	// make a patch to add the document to the window in the start of the shift direction
	addPatch := json_patch.JSONPatch{
		Op:    "add",
		Path:  fmt.Sprintf("/result/%s", addIndex),
		Value: newDocument[0],
	}
	patches = append(patches, addPatch)

	// insert the _id at the correct position
	insertIndex := 0
	if dir == ShiftDown {
		insertIndex = len(watchQuery.WatchedDocuments)
	}

	watchQuery.SaveAddDocumentToWindow(dbService, newDocument[0], insertIndex)

	return patches
}

func GetSingleDocumentInWindowOnIndex(dbService *db.DBService, watchQuery *db.WatchQuery, index int) []types.Document {
	// unmarshal options
	optionsMap := map[string]interface{}{}
	if err := json.Unmarshal([]byte(watchQuery.Options), &optionsMap); err != nil {
		fmt.Printf("Failed to unmarshal find options: %v", err)
		return nil
	}

	// set the options up to only return one document
	optionsMap["skip"] = index
	optionsMap["limit"] = 1
	optionsJSON, err := json.Marshal(optionsMap)
	if err != nil {
		fmt.Printf("Failed to marshal find options: %v", err)
		return nil
	}

	// query the new document
	documents := dbService.Connection.Query(watchQuery.Collection, watchQuery.Query, string(optionsJSON))

	// Important: If we are at the end of the cursor the query should return an empty array as skip is automatically the number of documents in the system. This is an absolute edgecase as it can only happen if the last window in the cursor is completly full but there are not further documents
	if len(documents) == 0 {
		return nil
	}

	return documents
}
