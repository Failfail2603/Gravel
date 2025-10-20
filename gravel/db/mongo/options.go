package mongo

import (
	"encoding/json"
	"gravel/types"

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
		} else {
			findOptions.SetSkip(0)
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

func extractSortFields(findOptions options.FindOptions) []types.SortField {
	var sortFields []types.SortField

	if findOptions.Sort != nil {
		if sort, ok := findOptions.Sort.(map[string]interface{}); ok {
			for field, orderVal := range sort {
				order := 1 // default ascending
				if orderInt, ok := orderVal.(int); ok {
					order = orderInt
				}
				sortFields = append(sortFields, types.SortField{
					Field: field,
					Order: order,
				})
			}
		}
	}

	return sortFields
}
