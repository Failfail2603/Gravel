package mongo

import (
	"encoding/json"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
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
}

func parseQueryString(queryString string) (bson.M, error) {
	var filter bson.M
	if queryString == "" {
		return filter, nil
	}

	if err := json.Unmarshal([]byte(queryString), &filter); err != nil {
		log.Printf("Failed to parse query string: %v", err)
		return nil, err
	}
	return filter, nil
}

func getRelevantFieldsFromQueryObject(queryObject bson.M) ([]string, error) {
	// Extract relevant fields from filter
	filterFields := flattenObject(queryObject)

	// as a query can consist of multiple conditions and mongo specific keywords we need to remove the $ keywords from the fields as they are not relevant for the watchquery
	// Filter out MongoDB query keywords from filter fields
	var cleanedFields []string
	for _, field := range filterFields {
		isKeyword := false
		for _, keyword := range MONGO_QUERY_KEYWORDS {
			if strings.Contains(field, keyword) {
				isKeyword = true
				break
			}
		}
		if !isKeyword {
			cleanedFields = append(cleanedFields, field)
		}
	}
	filterFields = cleanedFields

	return filterFields, nil
}
