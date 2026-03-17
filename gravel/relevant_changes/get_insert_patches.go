package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func GetSimpleAddPatch(documentIndex int, document types.Document, shiftWindow ...bool) json_patch.JSONPatch {
	if len(shiftWindow) > 0 && shiftWindow[0] {
		return json_patch.JSONPatch{
			Op:    "add",
			Type:  "shift",
			Path:  json_patch.GetBasePatchPath(documentIndex),
			Value: document,
		}
	}

	return json_patch.JSONPatch{
		Op:    "add",
		Type:  "simple",
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
	matched, err := dbService.Connection.TestFilterWithDocument(watchQuery.Query, watchQuery.QueryInformation, types.Document(change.FullDocument.(primitive.M)))
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
	// check if above the window can even exist
	if watchQuery.QueryInformation.WindowStart != 0 {
		positionRelativeToFirst, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, 0, New)
		if err != nil {
			log.Printf("Error getting position of document relative to first: %v", err)
			return []json_patch.JSONPatch{}
		}

		// if the document is above the window we need to shift the window up
		if positionRelativeToFirst == 1 {
			return ShiftWindow(dbService, watchQuery, ShiftUp, change)
		}
	}

	// check if below the window
	positionRelativeToLast, err := GetPositionOfDocumentRelativeToIndex(dbService, watchQuery, change, -1, New)

	// if the document is below the last document and the window is not exhausted we can ignore it as it would fall below the window
	if positionRelativeToLast == -1 && !watchQuery.IsExhaustedWindow() {
		return []json_patch.JSONPatch{}
	}

	// add a remove patch for the last document if the window is not exhausted
	patches := []json_patch.JSONPatch{}
	if !watchQuery.IsExhaustedWindow() {
		patches = append(patches, GetSimpleRemovePatch(len(watchQuery.WatchedDocuments)-1))
	}

	// at this point the document is definitely in the window, so we can just add it
	patches = append(patches, addDocumentToWindow(dbService, watchQuery, change, documentInfo)...)

	return patches
}
