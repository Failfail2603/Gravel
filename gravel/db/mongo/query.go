package mongo

import (
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var MONGO_QUERY_KEYWORDS = []string{
	// Comparison operators
	"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
	// Logical operators
	"$and", "$or", "$not", "$nor",
	// Element operators
	"$exists", "$type",
	// Evaluation operators
	"$expr", "$jsonSchema", "$mod", "$regex", "$text", "$where",
	// Array operators
	"$all", "$elemMatch", "$size",
	// Bitwise operators
	"$bitsAllClear", "$bitsAllSet", "$bitsAnyClear", "$bitsAnySet",
	// Geospatial operators
	"$geoIntersects", "$geoWithin", "$near", "$nearSphere",
	// regex operators
	"$regex", "$options",
}

func isMongoKeyword(keyword string) bool {
	for _, k := range MONGO_QUERY_KEYWORDS {
		if k == keyword {
			return true
		}
	}
	return false
}

func parseQueryString(queryString string) (bson.M, error) {
	var filter bson.M
	if queryString == "" {
		return filter, nil
	}

	// Prefer Extended JSON parsing so types like ObjectId ({"$oid": "..."}) are
	// converted into native BSON types.
	if err := bson.UnmarshalExtJSON([]byte(queryString), true, &filter); err != nil {
		log.Printf("Failed to parse query string: %v", err)
		return nil, err
	}
	return filter, nil
}

func getRelevantFieldsFromQueryObject(queryObject bson.M) ([]string, error) {
	// Extract relevant fields from filter
	filterFields := flattenObject(queryObject)

	// as a query can consist of multiple conditions and mongo specific keywords we need to remove the $ keywords from the fields as they are not relevant for the watchquery
	// Extract actual field names by removing MongoDB query keywords from field paths
	var cleanedFields []string
	fieldSet := make(map[string]bool) // Use a set to avoid duplicates

	for _, field := range filterFields {
		// Split the field path by dots to process each segment
		segments := strings.Split(field, ".")
		var cleanedSegments []string

		for _, segment := range segments {
			// Check if this segment is a MongoDB keyword. If it is, skip it
			if isMongoKeyword(segment) {
				continue
			}

			// Only keep non-keyword segments
			cleanedSegments = append(cleanedSegments, segment)
		}

		// If we have any non-keyword segments, join them back and add to result
		if len(cleanedSegments) > 0 {
			cleanedField := strings.Join(cleanedSegments, ".")
			if !fieldSet[cleanedField] {
				fieldSet[cleanedField] = true
				cleanedFields = append(cleanedFields, cleanedField)
			}
		}
	}
	filterFields = cleanedFields

	return filterFields, nil
}

func isBasicIDLookupOrInOperation(queryObject bson.M) bool {
	if len(queryObject) != 1 {
		return false
	}

	idFilterRaw, exists := queryObject["_id"]
	if !exists {
		return false
	}

	switch idFilter := idFilterRaw.(type) {
	case bson.M:
		if len(idFilter) != 1 {
			return false
		}
		_, hasIn := idFilter["$in"]
		return hasIn
	case map[string]interface{}:
		if len(idFilter) != 1 {
			return false
		}
		_, hasIn := idFilter["$in"]
		return hasIn
	default:
		return true
	}
}

func extractIDsFromQueryObject(queryObject bson.M) []string {
	idFilterRaw, exists := queryObject["_id"]
	if !exists {
		return nil
	}

	convertIDToString := func(id interface{}) string {
		switch v := id.(type) {
		case string:
			return v
		case primitive.ObjectID:
			return v.Hex()
		default:
			return fmt.Sprintf("%v", v)
		}
	}

	extractIDsFromList := func(values []interface{}) []string {
		ids := make([]string, 0, len(values))
		for _, value := range values {
			ids = append(ids, convertIDToString(value))
		}
		return ids
	}

	switch idFilter := idFilterRaw.(type) {
	case bson.M:
		inValuesRaw, exists := idFilter["$in"]
		if !exists {
			return nil
		}

		switch inValues := inValuesRaw.(type) {
		case bson.A:
			return extractIDsFromList([]interface{}(inValues))
		case []interface{}:
			return extractIDsFromList(inValues)
		default:
			return nil
		}
	case map[string]interface{}:
		inValuesRaw, exists := idFilter["$in"]
		if !exists {
			return nil
		}

		switch inValues := inValuesRaw.(type) {
		case []interface{}:
			return extractIDsFromList(inValues)
		default:
			return nil
		}
	default:
		return []string{convertIDToString(idFilterRaw)}
	}
}
