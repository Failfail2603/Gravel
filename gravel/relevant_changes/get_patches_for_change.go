package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
)

func GetPatchesForChange(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {

	// check operation type
	// Skip if operation is not one of the supported types
	switch change.Operation {
	case "insert", "update", "delete", "replace":
		// These operations are relevant, continue processing
	default:
		return []json_patch.JSONPatch{}
	}

	// first trivial check. If the collection is not the same we can skip it as the change will never be relevant
	if watchQuery.Collection != change.Collection {
		return []json_patch.JSONPatch{}
	}

	// get individual updates for change after we did basic checks as this can get quite heavy
	change.Updates = ExtractFieldUpdates(change)

	patches := []json_patch.JSONPatch{}

	switch change.Operation {
	case "update":
		patches = GetUpdatePatches(dbService, watchQuery, change)
	case "insert":
		patches = GetInsertPatches(dbService, watchQuery, change)
	case "delete":
		patches = GetRemovePatches(dbService, watchQuery, change)
	case "replace":
		patches = GetReplacePatches(dbService, watchQuery, change)
	}

	// update the watchqueries internal document state with the patches
	watchQuery.SavePatches(dbService, patches)

	return patches

}
