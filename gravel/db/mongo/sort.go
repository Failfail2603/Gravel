package mongo

import (
	"gravel/types"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func getSortValuesFromDocument(document types.Document, sortFields []types.SortField) []interface{} {
	result := make([]interface{}, len(sortFields))

	for i, sortField := range sortFields {
		result[i] = GetValueByPath(document, sortField.Field)
	}

	return result
}

func getNewPositionForDocument(documents []types.WatchedDocument, oldIndex int, sortFields []types.SortField) int {
	if oldIndex < 0 || oldIndex >= len(documents) {
		return oldIndex // Invalid index, return as-is
	}

	doc := documents[oldIndex]

	// Binary search to find the correct position
	// We search in two parts: before oldIndex and after oldIndex

	// First, check if we need to move at all by comparing with neighbors
	if oldIndex > 0 && mongoSortingComparator(sortFields, doc, documents[oldIndex-1]) == 1 {
		// Document should move left (toward lower indices)
		// Binary search in range [0, oldIndex)
		left := 0
		right := oldIndex

		for left < right {
			mid := left + (right-left)/2
			if mongoSortingComparator(sortFields, doc, documents[mid]) == 1 {
				right = mid
			} else {
				left = mid + 1
			}
		}
		return left
	}

	if oldIndex < len(documents)-1 && mongoSortingComparator(sortFields, doc, documents[oldIndex+1]) == -1 {
		// Document should move right (toward higher indices)
		// Binary search in range (oldIndex, len(documents)]
		left := oldIndex + 1
		right := len(documents)

		for left < right {
			mid := left + (right-left)/2
			if mongoSortingComparator(sortFields, doc, documents[mid]) == -1 {
				left = mid + 1
			} else {
				right = mid
			}
		}
		// Since we're inserting after removal, adjust by -1
		return left - 1
	}

	// Document is already in correct position
	return oldIndex
}

// mongoSortingComparator compares two WatchedDocuments based on MongoDB sorting rules
// Returns: -1 if doc1 < doc2, 0 if equal, 1 if doc1 > doc2
func mongoSortingComparator(sortFields []types.SortField, doc1 types.WatchedDocument, doc2 types.WatchedDocument) int {
	// Compare each sort field in order
	for i, sortField := range sortFields {

		// TODO this should never happen as sorting should always be in the context of a single query
		// Ensure we have sort values for both documents
		if i >= len(doc1.SortValues) || i >= len(doc2.SortValues) {
			// If one document has fewer values, treat it as less than
			if len(doc1.SortValues) < len(doc2.SortValues) {
				return -1
			} else if len(doc1.SortValues) > len(doc2.SortValues) {
				return 1
			}
			return 0
		}

		val1 := doc1.SortValues[i]
		val2 := doc2.SortValues[i]

		// Compare the values
		cmp := compareValues(val1, val2)

		// If values are different, apply sort order and return
		if cmp != 0 {
			if sortField.Order == -1 {
				return -cmp // Reverse for descending
			}
			return cmp
		}
		// If equal, continue to next sort field
	}

	// All sort fields are equal
	return 0
}

// compareValues compares two values according to MongoDB type precedence and rules
func compareValues(v1, v2 interface{}) int {
	// Handle nil/null values - null is always less than any other value
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}

	// Get type priority for MongoDB comparison
	type1 := getMongoTypePriority(v1)
	type2 := getMongoTypePriority(v2)

	// If different types, compare by type priority
	if type1 != type2 {
		if type1 < type2 {
			return -1
		}
		return 1
	}

	// Same type - compare by value
	switch type1 {
	case 2: // Number
		return compareNumbers(v1, v2)
	case 3: // String
		return compareStrings(v1, v2)
	case 8: // Boolean
		return compareBooleans(v1, v2)
	case 9: // Date/Time
		return compareDates(v1, v2)
	default:
		// For other types, try string comparison as fallback
		s1 := toString(v1)
		s2 := toString(v2)
		return strings.Compare(s1, s2)
	}
}

// getMongoTypePriority returns the MongoDB type comparison priority
// Based on: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
func getMongoTypePriority(v interface{}) int {
	if v == nil {
		return 1 // Null/Undefined
	}

	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return 2 // Numbers
	case string:
		return 3 // String
	case map[string]interface{}:
		return 4 // Object
	case []interface{}:
		return 5 // Array
	case primitive.ObjectID:
		return 7 // ObjectID
	case bool:
		return 8 // Boolean
	case time.Time, primitive.DateTime:
		return 9 // Date
	default:
		return 10 // Other
	}
}

// compareNumbers compares two numeric values
func compareNumbers(v1, v2 interface{}) int {
	n1 := toFloat64(v1)
	n2 := toFloat64(v2)

	// Handle NaN
	if math.IsNaN(n1) && math.IsNaN(n2) {
		return 0
	}
	if math.IsNaN(n1) {
		return -1
	}
	if math.IsNaN(n2) {
		return 1
	}

	if n1 < n2 {
		return -1
	}
	if n1 > n2 {
		return 1
	}
	return 0
}

// compareStrings compares two string values lexicographically
func compareStrings(v1, v2 interface{}) int {
	s1, ok1 := v1.(string)
	s2, ok2 := v2.(string)

	if !ok1 || !ok2 {
		// Fallback to string conversion
		s1 = toString(v1)
		s2 = toString(v2)
	}

	return strings.Compare(s1, s2)
}

// compareBooleans compares two boolean values (false < true)
func compareBooleans(v1, v2 interface{}) int {
	b1, ok1 := v1.(bool)
	b2, ok2 := v2.(bool)

	if !ok1 || !ok2 {
		return 0
	}

	if b1 == b2 {
		return 0
	}
	if !b1 && b2 {
		return -1
	}
	return 1
}

// compareDates compares two date/time values
func compareDates(v1, v2 interface{}) int {
	t1 := toTime(v1)
	t2 := toTime(v2)

	if t1.Before(t2) {
		return -1
	}
	if t1.After(t2) {
		return 1
	}
	return 0
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		return math.NaN()
	}
}

// toTime converts various time representations to time.Time
func toTime(v interface{}) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t
	case primitive.DateTime:
		// MongoDB DateTime is milliseconds since Unix epoch
		return t.Time()
	case int64:
		// Assume Unix timestamp in milliseconds
		return time.Unix(0, t*int64(time.Millisecond))
	case float64:
		// Assume Unix timestamp in seconds
		return time.Unix(int64(t), 0)
	default:
		return time.Time{}
	}
}

// toString converts a value to string for fallback comparison
func toString(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	default:
		return ""
	}
}
