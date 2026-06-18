package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestSchemasHaveExpectedColumns(t *testing.T) {
	hasField := func(t *testing.T, schema *arrow.Schema, name string) {
		t.Helper()
		_, ok := schema.FieldsByName(name)
		assert.True(t, ok, "schema missing field %q", name)
	}

	assert.NotZero(t, tracesSchema().NumFields(), "traces schema empty")
	hasField(t, tracesSchema(), "SpanAttributes")
	hasField(t, logsSchema(), "Body")
	hasField(t, metricsGaugeSchema(), "Value")
	hasField(t, metricsSumSchema(), "IsMonotonic")
	hasField(t, metricsHistogramSchema(), "BucketCounts")
	hasField(t, metricsExpHistogramSchema(), "PositiveBucketCounts")
	hasField(t, metricsSummarySchema(), "ValueAtQuantiles")
}
