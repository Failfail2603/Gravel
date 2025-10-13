package relevant_changes

import (
	"encoding/json"
	"gravel/db"
	"log"
)

// isSingleFieldInRelevantArray checks if a field path matches any of the relevant fields
func isSingleFieldInRelevantArray(fieldPath string, relevantFields []string) bool {
	for _, relevantField := range relevantFields {
		// Exact match
		if fieldPath == relevantField {
			return true
		}

		// Check if the field is a nested field of a relevant field
		// e.g., if relevant field is "user" and updated field is "user.name"
		if len(fieldPath) > len(relevantField) &&
			fieldPath[:len(relevantField)] == relevantField &&
			fieldPath[len(relevantField)] == '.' {
			return true
		}

		// Check if the relevant field is a nested field of the updated field
		// e.g., if relevant field is "user.name" and updated field is "user"
		if len(relevantField) > len(fieldPath) &&
			relevantField[:len(fieldPath)] == fieldPath &&
			relevantField[len(fieldPath)] == '.' {
			return true
		}
	}
	return false
}

func isFieldRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) (bool, bool) {

	// if the projection is empty we need to watch the entire document so every change is relevant
	if len(watchQuery.QueryInformation.ProjectionFields) == 0 {
		log.Println("Projection is empty. Watching entire document. Every change is relevant")
		return true, true
	}

	// relevant fields are all fields that are projected, sorted and filtered. If any of these have changed we know the change is at least relevant to the watchquery
	relevantFields := watchQuery.QueryInformation.ProjectionFields
	relevantFields = append(relevantFields, watchQuery.QueryInformation.SortFields...)
	relevantFields = append(relevantFields, watchQuery.QueryInformation.FilterFields...)
	log.Println("Relevant fields: ", relevantFields)

	// The change.Document is already the full MongoDB change event
	// We need to properly handle the nested structure
	var docMap map[string]interface{}

	// First try direct casting (in case it's already a map)
	if directMap, ok := change.Document.(map[string]interface{}); ok {
		docMap = directMap
	} else {
		// If direct casting fails, marshal and unmarshal to handle nested structures
		docBytes, err := json.Marshal(change.Document)
		if err != nil {
			log.Println("Can't marshal document. Assuming relevant")
			return true, true
		}

		if err := json.Unmarshal(docBytes, &docMap); err != nil {
			log.Println("Can't unmarshal document. Assuming relevant")
			return true, true
		}
	}

	sortingFields := watchQuery.QueryInformation.SortFields

	// Extract updateDescription from the change document
	updateDesc, ok := docMap["updateDescription"].(map[string]interface{})
	if !ok {
		log.Println("Can't parse updateDescription. Assuming relevant")
		return true, true // If no updateDescription, assume it's relevant to be safe
	}

	// Check updated fields
	if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
		for fieldPath := range updatedFields {
			log.Println("Updated field: ", fieldPath)
			if isSingleFieldInRelevantArray(fieldPath, relevantFields) {
				fieldIsSortRelevant := isSingleFieldInRelevantArray(fieldPath, sortingFields)
				return true, fieldIsSortRelevant
			}
		}
	}

	// Check removed fields
	if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
		for _, field := range removedFields {
			log.Println("Removed field: ", field)
			if fieldStr, ok := field.(string); ok {
				if isSingleFieldInRelevantArray(fieldStr, relevantFields) {
					fieldIsSortRelevant := isSingleFieldInRelevantArray(fieldStr, sortingFields)
					return true, fieldIsSortRelevant
				}
			}
		}
	}

	// Check truncated arrays
	if truncatedArrays, ok := updateDesc["truncatedArrays"].([]interface{}); ok {
		for _, arrayInfo := range truncatedArrays {
			log.Println("Truncated array: ", arrayInfo)
			if arrayMap, ok := arrayInfo.(map[string]interface{}); ok {
				if field, ok := arrayMap["field"].(string); ok {
					if isSingleFieldInRelevantArray(field, relevantFields) {
						fieldIsSortRelevant := isSingleFieldInRelevantArray(field, sortingFields)
						return true, fieldIsSortRelevant
					}
				}
			}
		}
	}

	return false, false
}

func isDocumentRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

	// TODO make this better at the moment we have no window shifting
	// check if the update is relevant to the watched documents
	// check the change document id against the currently watched _ids
	if len(watchQuery.WatchedDocumentIds) == 0 {
		return false
	}

	for _, watchedID := range watchQuery.WatchedDocumentIds {
		if change.ID == watchedID {
			return true
		}
	}

	return false
}

// check if the update got made on any relevant field
func isUpdateRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

	isFieldChangedRelevant, isSortRelevant := isFieldRelevant(watchQuery, change)
	isDocumentRelevant := isDocumentRelevant(watchQuery, change)

	// the base case here is if a relevant field got changed. if not we can completly ignore the update
	// after that we check if the document is relevant by looking in our current window.
	// if the document is not in our window we need to check if the change was made on a sorted field
	return isFieldChangedRelevant && (isDocumentRelevant || isSortRelevant)
}
