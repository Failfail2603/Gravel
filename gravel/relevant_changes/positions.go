package relevant_changes

import (
	"gravel/db"
	"gravel/types"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DocumentType int

const (
	Old DocumentType = iota
	New
)

func GetPositionOfDocumentRelativeToIndex(dbService *db.DBService, watchQuery *db.WatchQuery, change *types.DBChangeStreamEvent, relativeTo int, docType DocumentType) (int, error) {
	// If WatchedDocuments and this method is called we define that a document is above the window everytime as we cannot compare against the window
	if len(watchQuery.WatchedDocuments) == 0 {
		return 1, nil
	}

	var documentInfo types.WatchedDocument
	var err error

	switch docType {
	case Old:
		documentInfo, err = dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocumentBeforeChange.(primitive.M)), watchQuery.QueryInformation)
	case New:
		documentInfo, err = dbService.Connection.GetWatchedDocumentInfo(types.Document(change.FullDocument.(primitive.M)), watchQuery.QueryInformation)
	}

	if err != nil {
		log.Printf("Error getting watched document info: %v", err)
		return -1, err
	}

	// -1 should be the end of the array
	if relativeTo == -1 {
		relativeTo = len(watchQuery.WatchedDocuments) - 1
	}

	// was the document before the change above the window?
	beforePositionRelativeToFirst := dbService.Connection.GetSortingOrder(documentInfo, watchQuery.WatchedDocuments[relativeTo], watchQuery.QueryInformation)

	return beforePositionRelativeToFirst, nil
}
