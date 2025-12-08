package relevant_changes

import (
	"encoding/json"
	"gravel/types"
	"log"
)

// ExtractFieldUpdates extracts individual field updates from a MongoDB change event.
// MongoDB change events contain different structures depending on the operation:
// - update: updateDescription.updatedFields and updateDescription.removedFields
// - insert: fullDocument contains all fields
// - replace: fullDocument contains all fields
// - delete: no field updates (document is removed)
//
// Example usage:
//
//	change := &types.DBChangeStreamEvent{
//	    Operation: "update",
//	    Document: map[string]interface{}{
//	        "updateDescription": map[string]interface{}{
//	            "updatedFields": map[string]interface{}{
//	                "name": "John",
//	                "age": 30,
//	            },
//	            "removedFields": []interface{}{"oldField"},
//	        },
//	    },
//	}
//	updates := ExtractFieldUpdates(change)
//	// Returns: [
//	//   {Field: "name", Value: "John", Operation: "set"},
//	//   {Field: "age", Value: 30, Operation: "set"},
//	//   {Field: "oldField", Value: nil, Operation: "unset"}
//	// ]
func ExtractFieldUpdates(change *types.DBChangeStreamEvent) []types.FieldUpdate {
	updates := []types.FieldUpdate{}

	// Convert Document to map for easier navigation
	var docMap map[string]interface{}
	if directMap, ok := change.FullUpdate.(map[string]interface{}); ok {
		docMap = directMap
	} else {
		// Marshal and unmarshal to handle nested structures
		docBytes, err := json.Marshal(change.FullUpdate)
		if err != nil {
			log.Printf("Failed to marshal change document: %v", err)
			return updates
		}
		if err := json.Unmarshal(docBytes, &docMap); err != nil {
			log.Printf("Failed to unmarshal change document: %v", err)
			return updates
		}
	}

	switch change.Operation {
	case "update":
		// Extract updated fields
		if updateDesc, ok := docMap["updateDescription"].(map[string]interface{}); ok {
			// Handle updatedFields
			if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
				for field, value := range updatedFields {
					updates = append(updates, types.FieldUpdate{
						Field:     field,
						Value:     value,
						Operation: "set",
					})
				}
			}

			// Handle removedFields
			if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
				for _, field := range removedFields {
					if fieldStr, ok := field.(string); ok {
						updates = append(updates, types.FieldUpdate{
							Field:     fieldStr,
							Value:     nil,
							Operation: "unset",
						})
					}
				}
			}
		}

	case "insert", "replace":
		// For insert and replace, extract all fields from fullDocument
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			flattenFields(fullDoc, "", &updates)
		}

	case "delete":
		// For delete, there are no field updates - the entire document is removed
		// We could potentially add a special update for the entire document deletion
		// but typically this is handled at the document level, not field level

	default:
		log.Printf("Unknown operation type: %s", change.Operation)
	}

	return updates
}

// flattenFields recursively flattens nested objects into dot-notation field paths
// e.g., {"user": {"name": "John", "age": 30}} becomes ["user.name", "user.age"]
func flattenFields(obj map[string]interface{}, prefix string, updates *[]types.FieldUpdate) {
	for key, value := range obj {
		fieldPath := key
		if prefix != "" {
			fieldPath = prefix + "." + key
		}

		// Check if value is a nested object
		if nestedObj, ok := value.(map[string]interface{}); ok {
			// Recursively flatten nested objects
			flattenFields(nestedObj, fieldPath, updates)
		} else {
			// Add leaf field
			*updates = append(*updates, types.FieldUpdate{
				Field:     fieldPath,
				Value:     value,
				Operation: "set",
			})
		}
	}
}
