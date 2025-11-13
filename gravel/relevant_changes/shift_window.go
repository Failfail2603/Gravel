package relevant_changes

import (
	"context"
	"encoding/json"
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"
	"time"
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

func ShiftWindow(dbService *db.DBService, watchQuery *db.WatchQuery, dir ShiftDirection, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	// early exit if the window is not shiftable
	if !isWindowShiftApplicable(watchQuery, dir) {
		return patches
	}

	// get single document from the cursor in which the window is shifting
	baseIndex := watchQuery.QueryInformation.WindowStart
	if dir == ShiftDown {
		baseIndex = watchQuery.QueryInformation.WindowEnd - 1
	}

	// Adjust index based on cumulative shifts in this ClusterTime batch
	// Shift up (positive offset): query progressively higher indices to get missed spots
	// Shift down (negative offset): query progressively lower indices to get missed spots
	index := baseIndex + change.BatchShiftOffset
	if change.BatchShiftOffset != 0 {
		log.Printf("Window shift: Adjusted query index from %d to %d (batch offset: %d)",
			baseIndex, index, change.BatchShiftOffset)
	}

	// query the new document using session context from change event
	newDocument := GetSingleDocumentOnIndex(dbService, watchQuery, change, index)

	// Important: If we are at the end of the cursor the query should return an empty array as skip is automatically the number of documents in the system. This is an absolute edgecase as it can only happen if the last window in the cursor is completly full but there are not further documents
	if len(newDocument) == 0 {
		return patches
	}

	// make a patch to delete the document from the window in the end of the shift direction
	removeIndex := 0
	if dir == ShiftUp {
		removeIndex = len(watchQuery.WatchedDocuments) - 1
	}

	deletePatch := GetSimpleRemovePatch(removeIndex)
	patches = append(patches, deletePatch)

	addIndex := 0
	if dir == ShiftDown {
		addIndex = -1
	}

	if dir == ShiftUp && change.BatchShiftOffset != 0 {
		// add the offset to the insertion index to get the correct position as we queried lower indices
		addIndex += change.BatchShiftOffset
	}

	// make a patch to add the document to the window in the start of the shift direction
	addPatch := GetSimpleAddPatch(addIndex, newDocument[0], true)
	patches = append(patches, addPatch)

	return patches
}

func GetSingleDocumentOnIndex(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, index int) []types.Document {
	start := time.Now()
	// Parse the original options preserving structure for fields we don't modify
	var optionsRaw struct {
		Sort       json.RawMessage        `json:"sort,omitempty"`
		Projection json.RawMessage        `json:"projection,omitempty"`
		Skip       *int                   `json:"skip,omitempty"`
		Limit      *int                   `json:"limit,omitempty"`
		Extra      map[string]interface{} `json:"-"`
	}

	// check the cache first
	if change.UpdateCache[index] != nil {
		return []types.Document{change.UpdateCache[index]}
	}

	if watchQuery.Options != "" {
		if err := json.Unmarshal([]byte(watchQuery.Options), &optionsRaw); err != nil {
			fmt.Printf("Failed to unmarshal find options: %v", err)
			return nil
		}
	}

	// Build new options with modified skip and limit
	newSkip := index
	newLimit := 1

	// Create new options struct preserving original fields
	modifiedOptions := struct {
		Sort       json.RawMessage `json:"sort,omitempty"`
		Projection json.RawMessage `json:"projection,omitempty"`
		Skip       int             `json:"skip"`
		Limit      int             `json:"limit"`
	}{
		Sort:       optionsRaw.Sort,
		Projection: optionsRaw.Projection,
		Skip:       newSkip,
		Limit:      newLimit,
	}

	optionsJSON, err := json.Marshal(modifiedOptions)
	if err != nil {
		fmt.Printf("Failed to marshal find options: %v", err)
		return nil
	}

	// query the new document using snapshot read concern at the change event's cluster time
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	documents := dbService.Connection.QueryWithEvent(ctx, change, watchQuery.Collection, watchQuery.Query, string(optionsJSON))

	// Important: If we are at the end of the cursor the query should return an empty array as skip is automatically the number of documents in the system. This is an absolute edgecase as it can only happen if the last window in the cursor is completly full but there are not further documents
	if len(documents) == 0 {
		return nil
	}

	// save the document to the cache
	change.UpdateCache[index] = documents[0]

	log.Printf("GetSingleDocumentOnIndex took %s\n", time.Since(start))
	return documents
}
