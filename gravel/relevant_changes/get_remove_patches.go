package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func GetSimpleRemovePatch(documentIndex int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:   "remove",
		Path: json_patch.GetBasePatchPath(documentIndex),
	}

}

func GetRemovePatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {

	// first check if the document is in the current window
	removedDocumentWasInWindow, documentIndex := watchQuery.IsDocumentInWindow(change.ID)

	// for a deletion there are only two cases

	// 1. the document is in the window -> remove it and get a new one to fill
	// the function itselfs checks if the window is exhausted or infinite and gets a new document accroding to that
	if removedDocumentWasInWindow {
		return explainPatches(watchQuery, removeSingleDocumentFromWindowAndRetrieveNewDocument(dbService, watchQuery, documentIndex, change), "Delete removed a document that was currently inside the watched window, so Gravel updated the result set.")
	}

	// 2. the document is above the window -> shift window down
	// we can first check if there can even be documents above the window
	if watchQuery.QueryInformation.WindowStart == 0 {
		return explainNoop(watchQuery, "Delete was ignored because the watchquery has no skipped documents above the current window.")
	}

	// after we know that the document is not in the window and could be above we need to check if the delete doc would even match the query
	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, watchQuery.QueryInformation, types.Document(change.FullDocumentBeforeChange.(primitive.M)))
	if err != nil {
		log.Printf("Error testing filter: %v", err)
		return []json_patch.JSONPatch{}
	}

	// if the document did not match we can disregard it
	if !matched {
		return explainNoop(watchQuery, "Delete was ignored because the removed document did not match the watchquery filter.")
	}

	// the document matched and was not in window so we check if it is really above the window
	beforePositionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, Old)
	if err != nil {
		log.Printf("Error getting position of old document relative to first: %v", err)
		return []json_patch.JSONPatch{}
	}

	// if the document is not above the window we can disregard it
	if beforePositionRelativeToFirst == -1 {
		return explainNoop(watchQuery, "Delete matched the filter but occurred below the watched window, so no patch was emitted.")
	}

	// if the document was above the window we need to shift down so we can fill the gap
	return explainPatches(watchQuery, ShiftWindow(dbService, watchQuery, ShiftDown, change), "Delete removed a matching document above the watched window, so Gravel shifted the window down.")
}
