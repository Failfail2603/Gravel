package mongo

import (
	"encoding/json"
	"gravel/types"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// parseFindOptionsString parses a JSON string of find options into a FindOptions struct
func parseFindOptionsString(findOptionsString string) (options.FindOptions, error) {
	findOptions := options.Find()

	// early return on empty string
	if findOptionsString == "" {
		return *findOptions, nil
	}

	// Use a struct to capture raw sort field
	var optionsRaw struct {
		Limit      *float64               `json:"limit"`
		Skip       *float64               `json:"skip"`
		Projection map[string]interface{} `json:"projection"`
		Sort       json.RawMessage        `json:"sort"`
	}

	if err := json.Unmarshal([]byte(findOptionsString), &optionsRaw); err != nil {
		return *findOptions, err
	}

	if optionsRaw.Limit != nil {
		findOptions.SetLimit(int64(*optionsRaw.Limit))
	}
	if optionsRaw.Skip != nil {
		findOptions.SetSkip(int64(*optionsRaw.Skip))
	} else {
		findOptions.SetSkip(0)
	}
	if optionsRaw.Projection != nil {
		findOptions.SetProjection(optionsRaw.Projection)
	}
	if len(optionsRaw.Sort) > 0 {
		// Parse sort into bson.D to preserve field order
		sortD, err := parseSortToBsonD(optionsRaw.Sort)
		if err == nil {
			findOptions.SetSort(sortD)
		}
	}

	return *findOptions, nil
}

// parseSortToBsonD parses a JSON sort object into bson.D, preserving field order
func parseSortToBsonD(sortJSON json.RawMessage) (bson.D, error) {
	var sortArray []interface{}
	if err := json.Unmarshal(sortJSON, &sortArray); err == nil {
		// Handle array format
		sortD := bson.D{}
		for _, item := range sortArray {
			if pair, ok := item.([]interface{}); ok && len(pair) == 2 {
				if key, ok := pair[0].(string); ok {
					sortD = append(sortD, bson.E{Key: key, Value: pair[1]})
				}
			}
		}
		return sortD, nil
	}

	// Handle object format - manually parse to preserve order
	var sortMap map[string]interface{}
	if err := json.Unmarshal(sortJSON, &sortMap); err != nil {
		return nil, err
	}

	// Parse JSON object preserving key order using decoder
	sortD := bson.D{}
	decoder := json.NewDecoder(strings.NewReader(string(sortJSON)))
	decoder.UseNumber()
	
	// Read opening brace
	t, err := decoder.Token()
	if err != nil || t != json.Delim('{') {
		// Fallback to unordered map if parsing fails
		for k, v := range sortMap {
			sortD = append(sortD, bson.E{Key: k, Value: v})
		}
		return sortD, nil
	}

	// Read key-value pairs in order
	for decoder.More() {
		// Read key
		t, err := decoder.Token()
		if err != nil {
			break
		}
		key, ok := t.(string)
		if !ok {
			break
		}

		// Read value
		var value interface{}
		if err := decoder.Decode(&value); err != nil {
			break
		}

		// Convert json.Number to float64 if applicable
		if num, ok := value.(json.Number); ok {
			if floatVal, err := num.Float64(); err == nil {
				value = floatVal
			}
		}

		sortD = append(sortD, bson.E{Key: key, Value: value})
	}

	return sortD, nil
}

func extractSortFields(findOptions options.FindOptions) []types.SortField {
	var sortFields []types.SortField

	if findOptions.Sort != nil {
		if sortD, ok := findOptions.Sort.(bson.D); ok {
			for _, elem := range sortD {
				order := 1 // default ascending

				if orderFloat, ok := elem.Value.(float64); ok {
					order = int(orderFloat)
				} else if orderInt, ok := elem.Value.(int); ok {
					order = orderInt
				}

				sortFields = append(sortFields, types.SortField{
					Field: elem.Key,
					Order: order,
				})
			}
		}
	}

	return sortFields
}
