package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"log"
)

func GetPatchesForChange(dbService *db.DBService, watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) []json_patch.JSONPatch {
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

	switch change.Operation {
	case "update":

		return patches
	case "insert":
		return patches
	default:
		return patches
	}

}

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
//		- false: check if the update is above the own window
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
