package mongo

import (
	"fmt"
	"gravel/types"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TODO potential bug if the user sorts after a field which is not in the document. In this case nil is returned and the sorting logic can handle this

// GetValueByPath retrieves a value from a nested map using a dot-separated path
func GetValueByPath(doc types.Document, path string) interface{} {
	// Split the path by dots
	keys := strings.Split(path, ".")

	var current interface{} = doc

	// Navigate through the nested structure
	for _, key := range keys {
		// Type assert current to types.Document (or map[string]interface{} for nested values)
		var currentMap map[string]interface{}
		var ok bool

		// Try types.Document first (for the top level), then map[string]interface{} (for nested maps)
		if docMap, isDoc := current.(types.Document); isDoc {
			currentMap = map[string]interface{}(docMap)
			ok = true
		} else {
			currentMap, ok = current.(map[string]interface{})
		}
		if !ok {
			// Path doesn't exist or is not traversable
			return nil
		}

		// Get the value at the current key
		value, exists := currentMap[key]
		if !exists {
			// Key doesn't exist
			return nil
		}

		current = value
	}

	// Convert ObjectID to string before returning
	if objectID, isObjectID := current.(primitive.ObjectID); isObjectID {
		return objectID.Hex()
	}

	return current
}

// GetIDFromEntry extracts the _id field from a document entry and converts it to a string.
// It handles different ID types (string, ObjectID, etc.) and returns an error if the ID cannot be extracted.
func GetIDFromEntry(entry types.Document) (string, error) {
	// Extract document ID, handling different types (string, ObjectID, etc.)
	if docID, exists := entry["_id"]; exists {
		switch v := docID.(type) {
		case string:
			return v, nil
		case primitive.ObjectID:
			return v.Hex(), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	}

	return "", fmt.Errorf("document missing _id field: %+v", entry)
}
