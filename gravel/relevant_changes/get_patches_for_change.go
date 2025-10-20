package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"
)

func GetPatchesForChange(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {
	patches := []json_patch.JSONPatch{}

	// check operation type
	// Skip if operation is not one of the supported types
	switch change.Operation {
	case "insert", "update", "delete", "replace":
		// These operations are relevant, continue processing
	default:
		log.Println("Change is not relevant. Unsupported Operation: ", change.Operation)
		return patches
	}

	// first trivial check. If the collection is not the same we can skip it as the change will never be relevant
	if watchQuery.Collection != change.Collection {
		log.Println("Change is not relevant. Wrong Collection: ", change.Collection)
		return patches
	}

	// get individual updates for change after we did basic checks as this can get quite heavy
	change.Updates = ExtractFieldUpdates(change)

	switch change.Operation {
	case "update":

		patches = append(patches, GetUpdatePatches(dbService, watchQuery, change)...)
	case "insert":

	case "delete":

	case "replace":

	}

	return patches

}
