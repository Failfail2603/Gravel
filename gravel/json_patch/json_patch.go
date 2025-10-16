package json_patch

import (
	"encoding/json"
	"fmt"
)

type JSONPatch struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
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
