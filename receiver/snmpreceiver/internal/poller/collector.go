// Package poller provides SNMP data collection for metric groups.
package poller

import (
	"fmt"
	"strings"
	"time"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/metrics"
)

// MetricGroupDef defines a group of metrics to collect from an SNMP agent.
type MetricGroupDef struct {
	Name             string
	Walk             string // OID subtree to walk (empty = scalar mode)
	Metrics          []MetricDef
	Attributes       []AttributeDef
	ScalarAttributes []AttributeDef
	Lookups          []LookupDef
}

// MetricDef defines a single metric within a metric group.
type MetricDef struct {
	OID         string
	MetricName  string
	Type        string
	Unit        string
	Description string
}

// AttributeDef defines an OID-based attribute to attach to data points.
type AttributeDef struct {
	OID  string
	Name string
}

// LookupDef defines a lookup to resolve index values to labels.
type LookupDef struct {
	SourceIndexes []string
	LookupOID     string
	TargetLabel   string
}

// Collect collects metrics for the given group from the given connection.
// If group.Walk is empty, scalar mode is used (GET). Otherwise, table mode is used (WALK).
func Collect(conn connection.Connection, group MetricGroupDef) ([]metrics.CollectedMetric, error) {
	if group.Walk == "" {
		return collectScalar(conn, group)
	}
	return collectTable(conn, group)
}

// CollectScalarAttributes fetches scalar attribute OIDs via GET and returns a map of name -> string value.
func CollectScalarAttributes(conn connection.Connection, group MetricGroupDef) (map[string]string, error) {
	if len(group.ScalarAttributes) == 0 {
		return map[string]string{}, nil
	}

	oids := make([]string, 0, len(group.ScalarAttributes))
	for _, attr := range group.ScalarAttributes {
		oids = append(oids, attr.OID)
	}

	values, err := conn.Get(oids)
	if err != nil {
		return nil, fmt.Errorf("get scalar attributes: %w", err)
	}

	result := make(map[string]string, len(group.ScalarAttributes))
	for _, attr := range group.ScalarAttributes {
		if v, ok := values[attr.OID]; ok {
			result[attr.Name] = fmt.Sprintf("%v", v)
		}
	}
	return result, nil
}

// extractIndex strips prefix+"." from fullOID and returns the remaining suffix.
func extractIndex(prefix, fullOID string) string {
	p := prefix + "."
	if strings.HasPrefix(fullOID, p) {
		return fullOID[len(p):]
	}
	return ""
}

// collectScalar performs SNMP GET on each metric OID and returns one CollectedMetric per metric with one DataPoint.
func collectScalar(conn connection.Connection, group MetricGroupDef) ([]metrics.CollectedMetric, error) {
	oids := make([]string, 0, len(group.Metrics))
	for _, m := range group.Metrics {
		oids = append(oids, m.OID)
	}

	values, err := conn.Get(oids)
	if err != nil {
		return nil, fmt.Errorf("scalar get: %w", err)
	}

	now := time.Now()
	result := make([]metrics.CollectedMetric, 0, len(group.Metrics))
	for _, m := range group.Metrics {
		v, ok := values[m.OID]
		if !ok {
			continue
		}
		result = append(result, metrics.CollectedMetric{
			MetricName:  m.MetricName,
			Type:        m.Type,
			Unit:        m.Unit,
			Description: m.Description,
			DataPoints: []metrics.DataPoint{
				{
					Value:      v,
					Attributes: map[string]string{},
					Timestamp:  now,
				},
			},
		})
	}
	return result, nil
}

// collectTable performs SNMP WALK on each metric OID and attribute OID, builds DataPoints indexed by OID suffix.
func collectTable(conn connection.Connection, group MetricGroupDef) ([]metrics.CollectedMetric, error) {
	if len(group.Metrics) == 0 {
		return nil, nil
	}

	// Walk each metric OID.
	metricWalks := make([]map[string]interface{}, len(group.Metrics))
	for i, m := range group.Metrics {
		w, err := conn.Walk(m.OID)
		if err != nil {
			return nil, fmt.Errorf("walk metric %s: %w", m.OID, err)
		}
		metricWalks[i] = w
	}

	// Extract indexes from the first metric's walk results.
	firstMetricOID := group.Metrics[0].OID
	indexes := make([]string, 0, len(metricWalks[0]))
	for fullOID := range metricWalks[0] {
		idx := extractIndex(firstMetricOID, fullOID)
		if idx != "" {
			indexes = append(indexes, idx)
		}
	}

	// Walk each attribute OID.
	attrWalks := make(map[string]map[string]interface{}, len(group.Attributes))
	for _, attr := range group.Attributes {
		w, err := conn.Walk(attr.OID)
		if err != nil {
			return nil, fmt.Errorf("walk attribute %s: %w", attr.OID, err)
		}
		attrWalks[attr.OID] = w
	}

	// Walk each lookup OID.
	lookupWalks := make(map[string]map[string]interface{}, len(group.Lookups))
	for _, lookup := range group.Lookups {
		w, err := conn.Walk(lookup.LookupOID)
		if err != nil {
			return nil, fmt.Errorf("walk lookup %s: %w", lookup.LookupOID, err)
		}
		lookupWalks[lookup.LookupOID] = w
	}

	now := time.Now()
	indexAttrName := group.Name + "_index"

	result := make([]metrics.CollectedMetric, 0, len(group.Metrics))
	for i, m := range group.Metrics {
		dps := make([]metrics.DataPoint, 0, len(indexes))
		for _, idx := range indexes {
			fullOID := m.OID + "." + idx
			v, ok := metricWalks[i][fullOID]
			if !ok {
				continue
			}

			attrs := map[string]string{
				indexAttrName: idx,
			}

			// Add attribute OID values.
			for _, attr := range group.Attributes {
				attrFullOID := attr.OID + "." + idx
				if av, ok := attrWalks[attr.OID][attrFullOID]; ok {
					attrs[attr.Name] = fmt.Sprintf("%v", av)
				}
			}

			// Add lookup label values.
			for _, lookup := range group.Lookups {
				lookupFullOID := lookup.LookupOID + "." + idx
				if lv, ok := lookupWalks[lookup.LookupOID][lookupFullOID]; ok {
					attrs[lookup.TargetLabel] = fmt.Sprintf("%v", lv)
				}
			}

			dps = append(dps, metrics.DataPoint{
				Value:      v,
				Attributes: attrs,
				Timestamp:  now,
			})
		}

		result = append(result, metrics.CollectedMetric{
			MetricName:  m.MetricName,
			Type:        m.Type,
			Unit:        m.Unit,
			Description: m.Description,
			DataPoints:  dps,
		})
	}

	return result, nil
}
