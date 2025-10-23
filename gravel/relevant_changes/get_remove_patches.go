package relevant_changes

import "gravel/json_patch"

func GetSimpleRemovePatch(documentIndex int) json_patch.JSONPatch {
	return json_patch.JSONPatch{
		Op:   "remove",
		Path: json_patch.GetBasePatchPath(documentIndex),
	}

}
