package main

import (
	"encoding/json"
	"gravel/db/shared"
	"log"
	"strings"
)

func parseChangeToJSONPatchString(event shared.DBChangeStreamEvent) string {
	var patches []map[string]interface{}

	// Convert the document to a map for easier processing
	docBytes, err := json.Marshal(event.Document)
	if err != nil {
		log.Printf("Failed to marshal document: %v", err)
		return "[]"
	}

	var docMap map[string]interface{}
	if err := json.Unmarshal(docBytes, &docMap); err != nil {
		log.Printf("Failed to unmarshal document: %v", err)
		return "[]"
	}

	switch strings.ToLower(event.Operation) {
	case "insert":
		// For insert operations, add the entire document
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			patches = append(patches, map[string]interface{}{
				"_id":   event.ID,
				"op":    "add",
				"path":  "",
				"value": fullDoc,
			})
		} else {
			// If no fullDocument, use the entire document
			patches = append(patches, map[string]interface{}{
				"_id":   event.ID,
				"op":    "add",
				"path":  "",
				"value": docMap,
			})
		}

	case "update":
		// For update operations, process the updateDescription
		if updateDesc, ok := docMap["updateDescription"].(map[string]interface{}); ok {
			// Handle updated fields
			if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
				for field, value := range updatedFields {
					patches = append(patches, map[string]interface{}{
						"_id":   event.ID,
						"op":    "replace",
						"path":  "/" + strings.ReplaceAll(field, ".", "/"),
						"value": value,
					})
				}
			}

			// Handle removed fields
			if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
				for _, field := range removedFields {
					if fieldStr, ok := field.(string); ok {
						patches = append(patches, map[string]interface{}{
							"_id":  event.ID,
							"op":   "remove",
							"path": "/" + strings.ReplaceAll(fieldStr, ".", "/"),
						})
					}
				}
			}

			// Handle truncated arrays
			if truncatedArrays, ok := updateDesc["truncatedArrays"].([]interface{}); ok {
				for _, arrayInfo := range truncatedArrays {
					if arrayMap, ok := arrayInfo.(map[string]interface{}); ok {
						if field, ok := arrayMap["field"].(string); ok {
							if newSize, ok := arrayMap["newSize"].(float64); ok {
								patches = append(patches, map[string]interface{}{
									"_id":   event.ID,
									"op":    "replace",
									"path":  "/" + strings.ReplaceAll(field, ".", "/") + "/length",
									"value": int(newSize),
								})
							}
						}
					}
				}
			}
		}

	case "replace":
		// For replace operations, replace the entire document
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			patches = append(patches, map[string]interface{}{
				"_id":   event.ID,
				"op":    "replace",
				"path":  "",
				"value": fullDoc,
			})
		}

	case "delete":
		// For delete operations, remove the entire document
		patches = append(patches, map[string]interface{}{
			"_id":  event.ID,
			"op":   "remove",
			"path": "",
		})

	default:
		log.Printf("Unknown operation type: %s", event.Operation)
		return "[]"
	}

	// Convert patches to JSON string
	patchBytes, err := json.Marshal(patches)
	if err != nil {
		log.Printf("Failed to marshal patches: %v", err)
		return "[]"
	}

	return string(patchBytes)
}
