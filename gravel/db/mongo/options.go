package mongo

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// parseFindOptionsString parses a JSON string of find options into a FindOptions struct
func parseFindOptionsString(findOptionsString string) (options.FindOptions, error) {
	findOptions := options.Find()

	// early return on empty string
	if findOptionsString == "" {
		return *findOptions, nil
	}

	var optionsMap map[string]interface{}
	if err := json.Unmarshal([]byte(findOptionsString), &optionsMap); err == nil {
		if limit, ok := optionsMap["limit"].(float64); ok {
			findOptions.SetLimit(int64(limit))
		}
		if skip, ok := optionsMap["skip"].(float64); ok {
			findOptions.SetSkip(int64(skip))
		}
		if projection, ok := optionsMap["projection"].(map[string]interface{}); ok {
			findOptions.SetProjection(projection)
		}
		if sort, ok := optionsMap["sort"].(map[string]interface{}); ok {
			findOptions.SetSort(sort)
		}
	} else {
		return *findOptions, err
	}

	return *findOptions, nil
}
