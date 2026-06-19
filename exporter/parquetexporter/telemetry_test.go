package parquetexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordRotationEmitsFilesRotated(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	ts := componenttest.NewNopTelemetrySettings()
	ts.MeterProvider = mp

	tel, err := newTelemetry(ts)
	require.NoError(t, err)

	tel.recordRotation(context.Background(), "traces", reasonRows, 10, 2048, 0.25)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "parquetexporter.files.rotated" {
				found = true
				sum := m.Data.(metricdata.Sum[int64])
				require.Len(t, sum.DataPoints, 1)
				assert.Equal(t, int64(1), sum.DataPoints[0].Value)
			}
		}
	}
	assert.True(t, found, "parquetexporter.files.rotated not emitted")
}

func TestClassifyError(t *testing.T) {
	assert.Equal(t, "io", classifyError(assert.AnError))
}
