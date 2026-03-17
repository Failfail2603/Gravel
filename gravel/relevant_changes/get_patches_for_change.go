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
		return explainNoop(watchQuery, "Change operation is not supported by Gravel watchquery patch generation.")
	}

	// first trivial check. If the collection is not the same we can skip it as the change will never be relevant
	if watchQuery.Collection != change.Collection {
		return explainNoop(watchQuery, "Change was ignored because it belongs to a different collection than this watchquery.")
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

	if len(patches) == 0 {
		patches = explainNoop(watchQuery, "Change was processed but did not affect the watched query result window.")
	}

	// update the watchqueries internal document state with the patches
	watchQuery.SavePatches(dbService, patches)

	return patches

}
