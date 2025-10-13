package relevant_changes

import "gravel/db"

func isInsertRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

	isFieldChangedRelevant, isDocumentRelevant := isFieldRelevant(watchQuery, change)

	return isFieldChangedRelevant && isDocumentRelevant
}
