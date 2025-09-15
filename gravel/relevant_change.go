package main

import (
	"encoding/json"
	"gravel/db"
	"log"
)

// isFieldRelevant checks if a field path matches any of the relevant fields
func isFieldRelevant(fieldPath string, relevantFields []string) bool {
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

// check if the update got made on any relevant field
func isUpdateRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

	// if the projection is empty we need to watch the entire document so every change is relevant
	if len(watchQuery.QueryInformation.ProjectionFields) == 0 {
		log.Println("Projection is empty. Watching entire document. Every change is relevant")
		return true
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
			return true
		}

		if err := json.Unmarshal(docBytes, &docMap); err != nil {
			log.Println("Can't unmarshal document. Assuming relevant")
			return true
		}
	}

	// Extract updateDescription from the change document
	updateDesc, ok := docMap["updateDescription"].(map[string]interface{})
	if !ok {
		log.Println("Can't parse updateDescription. Assuming relevant")
		return true // If no updateDescription, assume it's relevant to be safe
	}

	// Check updated fields
	if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
		for fieldPath := range updatedFields {
			log.Println("Updated field: ", fieldPath)
			if isFieldRelevant(fieldPath, relevantFields) {
				return true
			}
		}
	}

	// Check removed fields
	if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
		for _, field := range removedFields {
			log.Println("Removed field: ", field)
			if fieldStr, ok := field.(string); ok {
				if isFieldRelevant(fieldStr, relevantFields) {
					return true
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
					if isFieldRelevant(field, relevantFields) {
						return true
					}
				}
			}
		}
	}

	return false
}

func isChangeRelevant(watchQuery *db.WatchQuery, change *db.DBChangeStreamEvent) bool {

	// check operation type
	// Skip if operation is not one of the supported types
	switch change.Operation {
	case "insert", "update", "delete", "replace":
		// These operations are relevant, continue processing
	default:
		log.Println("Change is not relevant. Unsupported Operation: ", change.Operation)
		return false
	}

	// first trivial check. If the collection is not the same we can skip it as the change will never be relevant
	if watchQuery.Collection != change.Collection {
		log.Println("Change is not relevant. Wrong Collection: ", change.Collection)
		return false
	}

	switch change.Operation {
	case "update":
		updateRelevant := isUpdateRelevant(watchQuery, change)
		log.Println("Is \"update\" event. Update relevant? ", updateRelevant)
		return updateRelevant
	default:
	}

	// TODO if the filter is directly on _ids we can just check if the change was made on the same _id
	// an insertion will never be happening at this point as the _id is new and cannot be possibly watched

	return true
}
