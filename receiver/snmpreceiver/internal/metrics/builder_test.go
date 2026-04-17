package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var testTime = time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

func TestBuildGaugeMetric(t *testing.T) {
	resourceAttrs := map[string]string{
		"custom.attr": "custom-value",
	}
	collected := []CollectedMetric{
		{
			MetricName:  "system.cpu.utilization",
			Type:        "gauge",
			Unit:        "%",
			Description: "CPU utilization",
			DataPoints: []DataPoint{
				{
					Value:      int64(42),
					Attributes: map[string]string{},
					Timestamp:  testTime,
				},
			},
		},
	}

	md := BuildMetrics("192.168.1.1", 161, resourceAttrs, collected)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	rm := md.ResourceMetrics().At(0)

	// Verify resource attributes
	attrs := rm.Resource().Attributes()
	hostVal, ok := attrs.Get("snmp.host")
	require.True(t, ok)
	assert.Equal(t, "192.168.1.1", hostVal.Str())

	portVal, ok := attrs.Get("snmp.port")
	require.True(t, ok)
	assert.Equal(t, int64(161), portVal.Int())

	customVal, ok := attrs.Get("custom.attr")
	require.True(t, ok)
	assert.Equal(t, "custom-value", customVal.Str())

	// Verify scope
	require.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)
	assert.Equal(t, scopeName, sm.Scope().Name())

	// Verify metric
	require.Equal(t, 1, sm.Metrics().Len())
	m := sm.Metrics().At(0)
	assert.Equal(t, "system.cpu.utilization", m.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, m.Type())

	// Verify data point value
	require.Equal(t, 1, m.Gauge().DataPoints().Len())
	dp := m.Gauge().DataPoints().At(0)
	assert.Equal(t, int64(42), dp.IntValue())
}

func TestBuildCounterMetric(t *testing.T) {
	collected := []CollectedMetric{
		{
			MetricName:  "interface.octets.in",
			Type:        "counter",
			Unit:        "By",
			Description: "Inbound octets",
			DataPoints: []DataPoint{
				{
					Value:      int64(1000),
					Attributes: map[string]string{"interface_name": "eth0"},
					Timestamp:  testTime,
				},
				{
					Value:      int64(2000),
					Attributes: map[string]string{"interface_name": "eth1"},
					Timestamp:  testTime,
				},
			},
		},
	}

	md := BuildMetrics("10.0.0.1", 161, nil, collected)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())

	m := sm.Metrics().At(0)
	assert.Equal(t, pmetric.MetricTypeSum, m.Type())

	s := m.Sum()
	assert.True(t, s.IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, s.AggregationTemporality())

	require.Equal(t, 2, s.DataPoints().Len())

	dp0 := s.DataPoints().At(0)
	assert.Equal(t, int64(1000), dp0.IntValue())
	ifaceVal0, ok := dp0.Attributes().Get("interface_name")
	require.True(t, ok)
	assert.Equal(t, "eth0", ifaceVal0.Str())

	dp1 := s.DataPoints().At(1)
	assert.Equal(t, int64(2000), dp1.IntValue())
	ifaceVal1, ok := dp1.Attributes().Get("interface_name")
	require.True(t, ok)
	assert.Equal(t, "eth1", ifaceVal1.Str())
}

func TestBuildUpDownCounterMetric(t *testing.T) {
	collected := []CollectedMetric{
		{
			MetricName:  "process.count",
			Type:        "up_down_counter",
			Unit:        "{processes}",
			Description: "Number of running processes",
			DataPoints: []DataPoint{
				{
					Value:      int64(15),
					Attributes: map[string]string{},
					Timestamp:  testTime,
				},
			},
		},
	}

	md := BuildMetrics("10.0.0.2", 161, nil, collected)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())

	m := sm.Metrics().At(0)
	assert.Equal(t, pmetric.MetricTypeSum, m.Type())

	s := m.Sum()
	assert.False(t, s.IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, s.AggregationTemporality())

	require.Equal(t, 1, s.DataPoints().Len())
	assert.Equal(t, int64(15), s.DataPoints().At(0).IntValue())
}

func TestBuildMetricsFloatValue(t *testing.T) {
	collected := []CollectedMetric{
		{
			MetricName:  "system.temperature",
			Type:        "gauge",
			Unit:        "Cel",
			Description: "System temperature",
			DataPoints: []DataPoint{
				{
					Value:      float64(36.6),
					Attributes: map[string]string{},
					Timestamp:  testTime,
				},
			},
		},
	}

	md := BuildMetrics("10.0.0.3", 161, nil, collected)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())

	m := sm.Metrics().At(0)
	assert.Equal(t, pmetric.MetricTypeGauge, m.Type())

	require.Equal(t, 1, m.Gauge().DataPoints().Len())
	dp := m.Gauge().DataPoints().At(0)
	assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, dp.ValueType())
	assert.InDelta(t, 36.6, dp.DoubleValue(), 0.0001)
}

func TestBuildMetricsEmpty(t *testing.T) {
	md := BuildMetrics("10.0.0.4", 161, nil, nil)
	assert.Equal(t, 0, md.ResourceMetrics().Len())

	md2 := BuildMetrics("10.0.0.4", 161, nil, []CollectedMetric{})
	assert.Equal(t, 0, md2.ResourceMetrics().Len())
}
