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

	// a downshift is always applicable on a windows query. A downshift could lead to an exhausted window or to no documents at all in the window but we dont care
	if dir == ShiftDown {
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

	// assemble the delete patch before getting a new document as a window shift will always try to delete a document

	// make a patch to delete the document from the window in the end of the shift direction
	// for up: only delete if the window is not exhausted as there are spaces available
	// for down: always delete as there is no space available
	if dir == ShiftUp && !watchQuery.IsExhaustedWindow() {
		deletePatch := GetSimpleRemovePatch(len(watchQuery.WatchedDocuments) - 1)
		patches = append(patches, deletePatch)
	} else if dir == ShiftDown {
		deletePatch := GetSimpleRemovePatch(0)
		patches = append(patches, deletePatch)
	}

	// query the new document using session context from change event
	newDocument := GetSingleDocumentOnIndex(dbService, watchQuery, change, index)

	// Important: If we are at the end of the cursor the query should return an empty array as skip is automatically the number of documents in the system. This is an absolute edgecase as it can only happen if the last window in the cursor is completly full but there are not further documents
	// on a shift up on an empty window were the skip is so high that the query returns an empty array this is also the case. Here we just return a empty patch array as we cannot shift the window
	if len(newDocument) == 0 {
		return patches
	}

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

	return documents
}
