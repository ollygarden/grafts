package parquetexporter

import (
	"encoding/json"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// attributesToJSON serializes an attribute map to a JSON object string.
// pcommon.Map.AsRaw() returns a map[string]any with Go-native scalar values
// and nested maps/slices, which json.Marshal handles directly. An empty map
// or any marshal failure yields "{}". Non-finite float64 values (NaN, ±Inf)
// that json.Marshal rejects are replaced with null rather than losing all attributes.
func attributesToJSON(m pcommon.Map) string {
	if m.Len() == 0 {
		return "{}"
	}
	raw := m.AsRaw()
	b, err := json.Marshal(raw)
	if err == nil {
		return string(b)
	}
	sanitized := sanitizeForJSON(raw).(map[string]any)
	b, err = json.Marshal(sanitized)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// sanitizeForJSON recursively replaces non-finite float64 values with nil.
func sanitizeForJSON(v any) any {
	switch val := v.(type) {
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil
		}
		return val
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, v2 := range val {
			out[k] = sanitizeForJSON(v2)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, v2 := range val {
			out[i] = sanitizeForJSON(v2)
		}
		return out
	default:
		return v
	}
}
