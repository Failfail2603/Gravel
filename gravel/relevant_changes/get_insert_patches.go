package relevant_changes

import "gravel/json_patch"

func GetSimpleAddPatch(documentIndex int, document interface{}) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:    "add",
		Path:  json_patch.GetBasePatchPath(documentIndex),
		Value: document,
	}
}
