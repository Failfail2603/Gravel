package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func GetSimpleAddPatch(documentIndex int, document interface{}) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:    "add",
		Path:  json_patch.GetBasePatchPath(documentIndex),
		Value: document,
	}
}

func GetInsertPatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	// check if the new document matches the filter
	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, types.Document(change.FullDocument.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter: %v", err)
		return []json_patch.JSONPatch{}
	}

	// if the document does not match the filter we can ignore it
	if !matched {
		return []json_patch.JSONPatch{}
	}

	// if the document matches the filter we need to check if it is in the window
	// if it is not in the window we need to check if it is above the window
	// if it is above the window we need to shift the window up
	// if it is below the window we need to shift the window down
	// if it is in the window we need to check if it is above the window
	// if it is above the window we need to shift the window up
	// if it is below the window we need to shift the window down

	return patches
}
