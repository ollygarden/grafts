package parquetexporter

import (
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// attributesToJSON serializes an attribute map to a JSON object string.
// pcommon.Map.AsRaw() returns a map[string]any with Go-native scalar values
// and nested maps/slices, which json.Marshal handles directly. An empty map
// or any marshal failure yields "{}".
func attributesToJSON(m pcommon.Map) string {
	if m.Len() == 0 {
		return "{}"
	}
	b, err := json.Marshal(m.AsRaw())
	if err != nil {
		return "{}"
	}
	return string(b)
}
