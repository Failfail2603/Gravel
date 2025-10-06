package main

import (
	"encoding/json"
	"fmt"
	"gravel/db"
	"log"
	"strings"
)

func getBasePathWithIndex(watchQuery *db.WatchQuery, id string) string {
	// find the id in the array to get the index
	for i, docID := range watchQuery.WatchedDocumentIds {
		if docID == id {
			return fmt.Sprintf("/result/%d", i)
		}
	}
	return ""
}

func parseChangeToJSONPatchString(watchQuery *db.WatchQuery, event *db.DBChangeStreamEvent) string {
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
		// TODO we need to find the positions on which to add the document in the array

		// For insert operations, add the entire document
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			patches = append(patches, map[string]interface{}{
				"op":    "add",
				"path":  "",
				"value": fullDoc,
			})
		} else {
			// If no fullDocument, use the entire document
			patches = append(patches, map[string]interface{}{
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
						"op":    "replace",
						"path":  getBasePathWithIndex(watchQuery, event.ID) + "/" + strings.ReplaceAll(field, ".", "/"),
						"value": value,
					})
				}
			}

			// Handle removed fields
			if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
				for _, field := range removedFields {
					if fieldStr, ok := field.(string); ok {
						patches = append(patches, map[string]interface{}{
							"op":   "remove",
							"path": "/" + strings.ReplaceAll(fieldStr, ".", "/"),
						})
					}
				}
			}

			// Handle truncated arrays
			// You probably do not need this in normal operation
			if truncatedArrays, ok := updateDesc["truncatedArrays"].([]interface{}); ok {
				for _, arrayInfo := range truncatedArrays {
					if arrayMap, ok := arrayInfo.(map[string]interface{}); ok {
						if field, ok := arrayMap["field"].(string); ok {
							if newSize, ok := arrayMap["newSize"].(float64); ok {
								patches = append(patches, map[string]interface{}{
									"op":    "replace",
									"path":  getBasePathWithIndex(watchQuery, event.ID) + "/" + strings.ReplaceAll(field, ".", "/") + "/length",
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
				"op":    "replace",
				"path":  getBasePathWithIndex(watchQuery, event.ID),
				"value": fullDoc,
			})
		}

	case "delete":
		// For delete operations, remove the entire document
		patches = append(patches, map[string]interface{}{
			"op":   "remove",
			"path": getBasePathWithIndex(watchQuery, event.ID),
		})

		// TODO if a document is removed we need to fill the window up with a possible new value
		// we can do this by sending a new query to gravel which only returns the end document of the window
		// the new element will alyways be added at the end of the window
		// 1-----				1-----
		// 2-----				2-----
		// 3----- -> D	4-----
		// 4-----	I ->	5-----

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
