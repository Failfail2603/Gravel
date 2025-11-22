package relevant_changes

import (
	"gravel/db"
	"gravel/types"
	"regexp"
	"strings"
)

// removeArrayIndices removes numeric array indices from a field path
// e.g., "roles.0.role" becomes "roles.role"
// e.g., "users.2.addresses.1.city" becomes "users.addresses.city"
var arrayIndexRegex = regexp.MustCompile(`\.\d+\.`)

func removeArrayIndices(fieldPath string) string {
	// Replace all occurrences of .NUMBER. with a single dot
	normalized := arrayIndexRegex.ReplaceAllString(fieldPath, ".")

	// Handle trailing array index (e.g., "roles.0")
	normalized = regexp.MustCompile(`\.\d+$`).ReplaceAllString(normalized, "")

	return normalized
}

// isSingleFieldInRelevantArray checks if a field path matches any of the relevant fields
// a field patch will be a dot joined path like "user.name"
// the relevant fields will be constructed the same way to allow for nested fields
// so "user.name" will match "user.name"
// the edgecase here is if we have an update on an entire nested object like user.
// so the fieldPath user should also match "user.name" if "user.name" is in the relevant fields
// It also handles array indices: "roles.0.role" will match "roles.role"
func isSingleFieldInRelevantArray(fieldPath string, relevantFields []string) bool {
	// Normalize the fieldPath by removing array indices
	normalizedFieldPath := removeArrayIndices(fieldPath)

	for _, relevantField := range relevantFields {
		// Exact match (with normalized field path)
		if normalizedFieldPath == relevantField {
			return true
		}

		// Also check exact match with original path (in case relevantField has indices too)
		if fieldPath == relevantField {
			return true
		}

		// check if the normalizedFieldPath is a subpath of the relevant fields
		// so "user.address" should match "user.address.city"
		// we cannot use contain as the path at the start must match exactly
		if strings.HasPrefix(relevantField, normalizedFieldPath+".") {
			return true
		}

		// check if the normalizedFieldPath is a parent of the relevant field
		if strings.HasPrefix(normalizedFieldPath, relevantField+".") {
			return true
		}
	}
	return false
}

func IsProjectedField(watchQuery *db.WatchQuery, singleUpdate *types.FieldUpdate) bool {
	return isSingleFieldInRelevantArray(singleUpdate.Field, watchQuery.QueryInformation.ProjectionFields)
}

func IsFilteredField(watchQuery *db.WatchQuery, singleUpdate *types.FieldUpdate) bool {
	return isSingleFieldInRelevantArray(singleUpdate.Field, watchQuery.QueryInformation.FilterFields)
}

func IsSortedField(watchQuery *db.WatchQuery, singleUpdate *types.FieldUpdate) bool {
	var sortFields []string
	for _, sortField := range watchQuery.QueryInformation.SortFields {
		sortFields = append(sortFields, sortField.Field)
	}
	return isSingleFieldInRelevantArray(singleUpdate.Field, sortFields)
}

// func GetUpdatedFieldPaths(watchQuery *types.WatchQuery, change *types.DBChangeStreamEvent) []string {
// 	updatedFieldPaths := []string{}

// 	// Extract updateDescription from the change document
// 	updateDesc, ok := docMap["updateDescription"].(map[string]interface{})
// 	if !ok {
// 		log.Println("Can't parse updateDescription. Assuming relevant")
// 		return true, true // If no updateDescription, assume it's relevant to be safe
// 	}

// 	// Check updated fields
// 	if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
// 		for fieldPath := range updatedFields {
// 			log.Println("Updated field: ", fieldPath)
// 			updatedFieldPaths = append(updatedFieldPaths, fieldPath)
// 		}
// 	}

// 	// Check removed fields
// 	if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
// 		for _, field := range removedFields {
// 			log.Println("Removed field: ", field)
// 			if fieldStr, ok := field.(string); ok {
// 				updatedFieldPaths = append(updatedFieldPaths, fieldStr)
// 			}
// 		}
// 	}

// 	// Check truncated arrays
// 	if truncatedArrays, ok := updateDesc["truncatedArrays"].([]interface{}); ok {
// 		for _, arrayInfo := range truncatedArrays {
// 			log.Println("Truncated array: ", arrayInfo)
// 			if arrayMap, ok := arrayInfo.(map[string]interface{}); ok {
// 				if field, ok := arrayMap["field"].(string); ok {
// 					if isSingleFieldInRelevantArray(field, relevantFields) {
// 						fieldIsSortRelevant := isSingleFieldInRelevantArray(field, sortingFields)
// 						return true, fieldIsSortRelevant
// 					}
// 				}
// 			}
// 		}
// 	}
// }
