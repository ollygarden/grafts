// Package metrics provides utilities for building pmetric.Metrics from
// collected SNMP data.
package metrics

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const scopeName = "go.olly.garden/grafts/receiver/snmpreceiver"

// DataPoint represents a single collected data point.
type DataPoint struct {
	Value      interface{}       // int, int64, uint32, uint64, float64, etc.
	Attributes map[string]string // metric attributes (e.g., interface_name)
	Timestamp  time.Time
}

// CollectedMetric represents collected data for one metric definition.
type CollectedMetric struct {
	MetricName  string
	Type        string // "counter", "gauge", "up_down_counter"
	Unit        string
	Description string
	DataPoints  []DataPoint
}

// BuildMetrics constructs pmetric.Metrics from collected SNMP data.
// host/port become resource attributes "snmp.host" and "snmp.port".
// resourceAttrs are additional resource attributes (from scalar_attributes).
// Returns empty Metrics if collected is empty.
func BuildMetrics(host string, port int, resourceAttrs map[string]string, collected []CollectedMetric) pmetric.Metrics {
	md := pmetric.NewMetrics()
	if len(collected) == 0 {
		return md
	}

	rm := md.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("snmp.host", host)
	res.Attributes().PutInt("snmp.port", int64(port))
	for k, v := range resourceAttrs {
		res.Attributes().PutStr(k, v)
	}

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)

	for _, cm := range collected {
		m := sm.Metrics().AppendEmpty()
		m.SetName(cm.MetricName)
		m.SetUnit(cm.Unit)
		m.SetDescription(cm.Description)

		switch cm.Type {
		case "gauge":
			g := m.SetEmptyGauge()
			for _, dp := range cm.DataPoints {
				ndp := g.DataPoints().AppendEmpty()
				ndp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
				setDataPointValue(ndp, dp.Value)
				for k, v := range dp.Attributes {
					ndp.Attributes().PutStr(k, v)
				}
			}
		case "counter":
			s := m.SetEmptySum()
			s.SetIsMonotonic(true)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, dp := range cm.DataPoints {
				ndp := s.DataPoints().AppendEmpty()
				ndp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
				setDataPointValue(ndp, dp.Value)
				for k, v := range dp.Attributes {
					ndp.Attributes().PutStr(k, v)
				}
			}
		case "up_down_counter":
			s := m.SetEmptySum()
			s.SetIsMonotonic(false)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, dp := range cm.DataPoints {
				ndp := s.DataPoints().AppendEmpty()
				ndp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
				setDataPointValue(ndp, dp.Value)
				for k, v := range dp.Attributes {
					ndp.Attributes().PutStr(k, v)
				}
			}
		default:
			// Unknown type: skip this metric but don't panic.
			_ = fmt.Sprintf("unknown metric type %q for metric %q", cm.Type, cm.MetricName)
		}
	}

	return md
}

// setDataPointValue assigns the typed value to a NumberDataPoint.
func setDataPointValue(dp pmetric.NumberDataPoint, value interface{}) {
	switch v := value.(type) {
	case int:
		dp.SetIntValue(int64(v))
	case int64:
		dp.SetIntValue(v)
	case uint:
		dp.SetIntValue(int64(v))
	case uint32:
		dp.SetIntValue(int64(v))
	case uint64:
		dp.SetIntValue(int64(v))
	case float32:
		dp.SetDoubleValue(float64(v))
	case float64:
		dp.SetDoubleValue(v)
	}
}
