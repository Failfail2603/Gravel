package mongo

import (
	"gravel/types"
	"strings"
)

func applyProjection(doc types.Document, options string) (types.Document, error) {

	// parse the find options to retrieve the projection as an object
	findOptions, err := parseFindOptionsString(options)
	if err != nil {
		return doc, err
	}

	// if no projection is specified, return the document as is
	if findOptions.Projection == nil {
		return doc, nil
	}

	projection, ok := findOptions.Projection.(map[string]interface{})
	if !ok {
		return doc, nil
	}

	// if projection is empty, return the document as is
	if len(projection) == 0 {
		return doc, nil
	}

	// flatten nested projection object into dot notation field paths
	fieldPaths := flattenObject(projection)

	// determine if _id is explicitly excluded
	includeID := true
	if idVal, exists := projection["_id"]; exists {
		if intVal, ok := idVal.(int); ok && intVal == 0 {
			includeID = false
		} else if floatVal, ok := idVal.(float64); ok && floatVal == 0 {
			includeID = false
		}
	}

	// create a new document with only the projected fields (inclusion projection)
	result := make(types.Document)

	for _, field := range fieldPaths {
		if field == "_id" {
			continue // handle _id separately
		}
		
		// handle nested fields using dot notation
		if val := getNestedValue(doc, field); val != nil {
			setNestedValue(result, field, val)
		}
	}

	// include _id if not explicitly excluded with 0
	if includeID {
		if id, exists := doc["_id"]; exists {
			result["_id"] = id
		}
	}

	return result, nil
}

// getNestedValue retrieves a value from a nested document using dot notation
// Example: getNestedValue(doc, "user.name") returns the value at doc["user"]["name"]
func getNestedValue(doc types.Document, path string) interface{} {
	parts := strings.Split(path, ".")
	
	var current interface{} = doc
	for _, part := range parts {
		// check if current is a map
		if m, ok := current.(map[string]interface{}); ok {
			val, exists := m[part]
			if !exists {
				return nil
			}
			current = val
		} else {
			return nil
		}
	}
	
	return current
}

// setNestedValue sets a value in a nested document using dot notation
// Example: setNestedValue(doc, "user.name", "John") sets doc["user"]["name"] = "John"
func setNestedValue(doc types.Document, path string, value interface{}) {
	parts := strings.Split(path, ".")
	
	// if it's a simple field (no dots), just set it directly
	if len(parts) == 1 {
		doc[parts[0]] = value
		return
	}
	
	// navigate/create nested structure
	current := doc
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		
		// check if the key exists and is a map
		if existing, exists := current[part]; exists {
			if m, ok := existing.(map[string]interface{}); ok {
				current = m
				continue
			}
		}
		
		// create a new nested map
		newMap := make(map[string]interface{})
		current[part] = newMap
		current = newMap
	}
	
	// set the final value
	current[parts[len(parts)-1]] = value
}
