package mongo

import (
	"gravel/types"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func applyProjection(doc types.Document, options string, nestedPath string) (types.Document, error) {

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

	// if nestedPath is provided, extract the nested projection at that path
	if nestedPath != "" {
		nestedProjection := extractNestedProjection(projection, nestedPath)
		if nestedProjection != nil {
			projection = nestedProjection
		} else {
			// if no projection exists at the nested path, return empty document
			return types.Document{}, nil
		}
	}

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

	// apply projection by walking the projection tree
	for field, projValue := range projection {
		if field == "_id" {
			continue // handle _id separately
		}

		// get the value from the document
		val, exists := doc[field]
		if !exists {
			continue
		}

		// check if this is a sub-projection (nested object)
		if subProj, ok := projValue.(map[string]interface{}); ok && len(subProj) > 0 {
			result[field] = applyProjectionToValue(val, subProj)
		} else {
			// primitive projection (1, 0, true, false) - include the field as is
			result[field] = val
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

// applyProjectionToValue applies a projection to any value (array, document, or primitive)
// Handles both MongoDB primitive types (primitive.A, primitive.M) and regular Go types ([]interface{}, map[string]interface{})
func applyProjectionToValue(val interface{}, projection map[string]interface{}) interface{} {

	// handle arrays: check both primitive.A (from MongoDB) and []interface{} (from Go)
	if arr, ok := val.(primitive.A); ok {
		// MongoDB primitive.A type
		projectedArray := make([]interface{}, 0, len(arr))
		for _, elem := range arr {
			projectedArray = append(projectedArray, applyProjectionToElement(elem, projection))
		}
		return projectedArray
	}

	if arr, ok := val.([]interface{}); ok {
		// Regular Go []interface{} type
		projectedArray := make([]interface{}, 0, len(arr))
		for _, elem := range arr {
			projectedArray = append(projectedArray, applyProjectionToElement(elem, projection))
		}
		return projectedArray
	}

	// handle nested documents: check both primitive.M (from MongoDB) and map[string]interface{} (from Go)
	if nestedDoc, ok := val.(primitive.M); ok {
		// MongoDB primitive.M type - convert to map[string]interface{}
		return applySubProjection(map[string]interface{}(nestedDoc), projection)
	}

	if nestedDoc, ok := val.(map[string]interface{}); ok {
		// Regular Go map[string]interface{} type
		return applySubProjection(nestedDoc, projection)
	}

	// for any other type, return as is
	return val
}

// applyProjectionToElement applies a projection to a single array element
func applyProjectionToElement(elem interface{}, projection map[string]interface{}) interface{} {
	// Check for primitive.M (MongoDB document)
	if elemDoc, ok := elem.(primitive.M); ok {
		return applySubProjection(map[string]interface{}(elemDoc), projection)
	}

	// Check for map[string]interface{} (regular Go document)
	if elemDoc, ok := elem.(map[string]interface{}); ok {
		return applySubProjection(elemDoc, projection)
	}

	// non-document elements are included as is
	return elem
}

// applySubProjection applies a projection to a document (used for nested documents and array elements)
func applySubProjection(doc map[string]interface{}, projection map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for field, projValue := range projection {
		val, exists := doc[field]
		if !exists {
			continue
		}

		// check if this is a sub-projection (nested object)
		if subProj, ok := projValue.(map[string]interface{}); ok && len(subProj) > 0 {
			result[field] = applyProjectionToValue(val, subProj)
		} else {
			// primitive projection - include the field as is
			result[field] = val
		}
	}

	return result
}

// extractNestedProjection extracts a nested projection object at the given dot-notation path
// Example: extractNestedProjection({address: {street: 1, city: 1}}, "address") returns {street: 1, city: 1}
func extractNestedProjection(projection map[string]interface{}, nestedPath string) map[string]interface{} {
	if nestedPath == "" {
		return projection
	}

	parts := strings.Split(nestedPath, ".")
	var current interface{} = projection

	// navigate through the path
	for _, part := range parts {
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

	// the final value should be a map representing the nested projection
	if nestedProj, ok := current.(map[string]interface{}); ok {
		return nestedProj
	}

	return nil
}
