package relevant_changes

import (
	"gravel/db"
	"gravel/json_patch"
)

func addExplanationsToPatch(patch json_patch.JSONPatch, explanations ...string) json_patch.JSONPatch {
	if len(explanations) == 0 {
		return patch
	}

	patch.Explanations = append(patch.Explanations, explanations...)
	return patch
}

func explainPatches(watchQuery *db.WatchQuery, patches []json_patch.JSONPatch, explanations ...string) []json_patch.JSONPatch {
	if !watchQuery.Explain || len(patches) == 0 || len(explanations) == 0 {
		return patches
	}

	explainedPatches := make([]json_patch.JSONPatch, 0, len(patches))
	for _, patch := range patches {
		explainedPatches = append(explainedPatches, addExplanationsToPatch(patch, explanations...))
	}
	return explainedPatches
}

func explainNoop(watchQuery *db.WatchQuery, explanations ...string) []json_patch.JSONPatch {
	if !watchQuery.Explain {
		return []json_patch.JSONPatch{}
	}

	return []json_patch.JSONPatch{{
		Op:           "noop",
		Type:         "explanation",
		Path:         "/result",
		Explanations: explanations,
	}}
}
