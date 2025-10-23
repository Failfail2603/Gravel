package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"
)

func GetPatchesForChange(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {

	// check operation type
	// Skip if operation is not one of the supported types
	switch change.Operation {
	case "insert", "update", "delete", "replace":
		// These operations are relevant, continue processing
	default:
		log.Println("Change is not relevant. Unsupported Operation: ", change.Operation)
		return []json_patch.JSONPatch{}
	}

	// first trivial check. If the collection is not the same we can skip it as the change will never be relevant
	if watchQuery.Collection != change.Collection {
		log.Println("Change is not relevant. Wrong Collection: ", change.Collection)
		return []json_patch.JSONPatch{}
	}

	// get individual updates for change after we did basic checks as this can get quite heavy
	change.Updates = ExtractFieldUpdates(change)

	switch change.Operation {
	case "update":
		return GetUpdatePatches(dbService, watchQuery, change)
	case "insert":
		return GetInsertPatches(dbService, watchQuery, change)
	case "delete":
		return GetRemovePatches(dbService, watchQuery, change)
	case "replace":
	}

	return []json_patch.JSONPatch{}

}
