package mongo

import (
	"log"

	"go.mongodb.org/mongo-driver/bson"
)

// flattenObject converts a projection object to a slice of dot-notated field paths
func flattenObject(object interface{}) []string {
	if object == nil {
		return []string{}
	}

	var fields []string

	log.Printf("%+q", object)

	switch proj := object.(type) {
	case map[string]interface{}:
		for key, value := range proj {
			fields = append(fields, flattenObjectRecursive(key, value, "")...)
		}
	case bson.M:
		for key, value := range proj {
			fields = append(fields, flattenObjectRecursive(key, value, "")...)
		}
	}

	return fields
}

// flattenObjectRecursive recursively flattens nested projection objects
func flattenObjectRecursive(key string, value interface{}, prefix string) []string {
	var fields []string

	fullKey := key
	if prefix != "" {
		fullKey = prefix + "." + key
	}

	switch v := value.(type) {
	case map[string]interface{}:
		// If it's an empty object, just add the key
		if len(v) == 0 {
			fields = append(fields, fullKey)
		} else {
			// If it's a nested object, recurse into it
			for nestedKey, nestedValue := range v {
				fields = append(fields, flattenObjectRecursive(nestedKey, nestedValue, fullKey)...)
			}
		}
	case bson.M:
		// If it's an empty bson.M, just add the key
		if len(v) == 0 {
			fields = append(fields, fullKey)
		} else {
			// If it's a nested bson.M, recurse into it
			for nestedKey, nestedValue := range v {
				fields = append(fields, flattenObjectRecursive(nestedKey, nestedValue, fullKey)...)
			}
		}
	case []interface{}:
		// If it's an array, check each element
		for _, arrayItem := range v {
			switch arrayItem.(type) {
			case map[string]interface{}, bson.M:
				// If array contains objects, recurse into each object
				// We use the same key since we're processing array elements
				fields = append(fields, flattenObjectRecursive(key, arrayItem, prefix)...)
			default:
				// If array contains primitives, we ignore them
				// No action needed
			}
		}
	case bson.A:
		// Handle bson.A (BSON array type)
		for _, arrayItem := range v {
			switch arrayItem.(type) {
			case map[string]interface{}, bson.M:
				// If array contains objects, recurse into each object
				fields = append(fields, flattenObjectRecursive(key, arrayItem, prefix)...)
			default:
				// If array contains primitives, we ignore them
			}
		}
	default:
		// For primitive values (1, 0, true, false), add the field path
		fields = append(fields, fullKey)
	}

	return fields
}
