package json_patch

import (
	"encoding/json"
	"fmt"
)

type JSONPatch struct {
	Op           string      `json:"op"`
	Type         string      `json:"type,omitempty"`
	From         string      `json:"from,omitempty"`
	Path         string      `json:"path"`
	Value        interface{} `json:"value"`
	Explanations []string    `json:"explanations,omitempty"`
}

// MarshalJSON implements custom JSON marshaling to conditionally omit the Value field
// for "remove" operations, as per JSON Patch RFC 6902 specification
func (p JSONPatch) MarshalJSON() ([]byte, error) {
	type Alias JSONPatch

	// For remove and noop operations, omit the value field entirely
	if p.Op == "remove" || p.Op == "noop" {
		aux := &struct {
			Op           string   `json:"op"`
			Type         string   `json:"type,omitempty"`
			From         string   `json:"from,omitempty"`
			Path         string   `json:"path"`
			Explanations []string `json:"explanations,omitempty"`
		}{
			Op:           p.Op,
			Type:         p.Type,
			From:         p.From,
			Path:         p.Path,
			Explanations: p.Explanations,
		}
		return json.Marshal(aux)
	}

	// For all other operations, include the value field
	return json.Marshal((Alias)(p))
}

func (p *JSONPatch) ToString() string {
	patchBytes, err := json.Marshal(p)
	if err != nil {
		fmt.Printf("Failed to marshal JSON patch: %v", err)
		return "{}"
	}
	return string(patchBytes)
}

func NewJSONPatch(patchString string) *JSONPatch {
	var patch JSONPatch
	err := json.Unmarshal([]byte(patchString), &patch)
	if err != nil {
		fmt.Printf("Failed to unmarshal JSON patch: %v", err)
		return nil
	}
	return &patch
}

func PatchArrayToString(patches []JSONPatch) string {
	patchBytes, err := json.Marshal(patches)
	if err != nil {
		fmt.Printf("Failed to marshal JSON patch array: %v", err)
		return "[]"
	}
	return string(patchBytes)
}

func GetBasePatchPath(index int) string {

	if index == -1 {
		return "/result/-"
	}

	return "/result/" + fmt.Sprint(index)
}
