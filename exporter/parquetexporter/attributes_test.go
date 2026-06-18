package parquetexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestAttributesToJSON(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("http.method", "GET")
	m.PutInt("retries", 3)
	m.PutBool("ok", true)

	got := attributesToJSON(m)
	// Map ordering is non-deterministic; assert all fragments are present.
	assert.Contains(t, got, `"http.method":"GET"`)
	assert.Contains(t, got, `"retries":3`)
	assert.Contains(t, got, `"ok":true`)
}

func TestAttributesToJSONEmpty(t *testing.T) {
	assert.Equal(t, "{}", attributesToJSON(pcommon.NewMap()))
}
