package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func GetSimpleAddPatch(documentIndex int, document types.Document) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:    "add",
		Path:  json_patch.GetBasePatchPath(documentIndex),
		Value: document,
	}
}

func addDocumentToWindow(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, documentInfo types.WatchedDocument) []json_patch.JSONPatch {

	patches := []json_patch.JSONPatch{}

	// get where the document should be inserted
	newIndex := dbService.Connection.GetPositionForDocumentInWindow(watchQuery.WatchedDocuments, documentInfo, watchQuery.QueryInformation.SortFields)

	// get the document -> use the fulldocument and project it
	newDocument, err := dbService.Connection.ProjectDocument(types.Document(change.FullDocument.(primitive.M)), watchQuery.Options, "")
	if err != nil {
		log.Printf("Error projecting document: %v", err)
		return []json_patch.JSONPatch{}
	}

	// add the patch
	patches = append(patches, GetSimpleAddPatch(newIndex, newDocument))

	return patches
}

func GetInsertPatches(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent) []json_patch.JSONPatch {

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

	// the document matches the query. check if it would fall into the window

	// get the sorting info
	documentInfo, err := dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocument.(primitive.M)), watchQuery.QueryInformation)
	if err != nil {
		log.Printf("Error getting watched document info: %v", err)
		return []json_patch.JSONPatch{}
	}

	// in the case of an infinite window we can always add the document
	if watchQuery.IsInfiniteWindow() {
		return addDocumentToWindow(dbService, watchQuery, change, documentInfo)
	}

	// in the case of a finite window we need to check if the document would fall into the window
	// check if above the window and can be even above the window if nothing can be above the window
	if watchQuery.QueryInformation.WindowStart != 0 {
		positionRelativeToFirst := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[0], watchQuery.QueryInformation)

		// if the document is above the window we need to shift the window up
		if positionRelativeToFirst == 1 {
			return ShiftWindow(dbService, watchQuery, ShiftUp, change)
		}
	}

	// check if below the window
	positionRelativeToLast := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[len(watchQuery.WatchedDocuments)-1], watchQuery.QueryInformation)

	// if the document is below, nothing would shift so ignore it
	if positionRelativeToLast == -1 {
		return []json_patch.JSONPatch{}
	}

	// at this point the document is definitely in the window, so we can just add it
	return addDocumentToWindow(dbService, watchQuery, change, documentInfo)
}
