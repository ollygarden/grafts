package poller

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
)

func TestCollectScalarMetricGroup(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(123456),
	})

	group := MetricGroupDef{
		Name: "system",
		Metrics: []MetricDef{
			{
				OID:        "1.3.6.1.2.1.1.3.0",
				MetricName: "sys_uptime",
				Type:       "gauge",
				Unit:       "ms",
			},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results, 1)

	cm := results[0]
	assert.Equal(t, "sys_uptime", cm.MetricName)
	assert.Equal(t, "gauge", cm.Type)
	assert.Equal(t, "ms", cm.Unit)
	require.Len(t, cm.DataPoints, 1)
	assert.Equal(t, uint32(123456), cm.DataPoints[0].Value)
}

func TestCollectTableMetricGroup(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.2.2.1.10.1": uint64(1000),
		"1.3.6.1.2.1.2.2.1.10.2": uint64(2000),
		"1.3.6.1.2.1.2.2.1.2.1":  "eth0",
		"1.3.6.1.2.1.2.2.1.2.2":  "eth1",
	})

	group := MetricGroupDef{
		Name: "if_traffic",
		Walk: "1.3.6.1.2.1.2.2.1",
		Metrics: []MetricDef{
			{
				OID:        "1.3.6.1.2.1.2.2.1.10",
				MetricName: "if_in_octets",
				Type:       "counter",
			},
		},
		Attributes: []AttributeDef{
			{
				OID:  "1.3.6.1.2.1.2.2.1.2",
				Name: "interface_description",
			},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results, 1)

	cm := results[0]
	assert.Equal(t, "if_in_octets", cm.MetricName)
	require.Len(t, cm.DataPoints, 2)

	// Build a map by index attr for order-independent checks.
	byIndex := map[string]interface{}{}
	byAttr := map[string]string{}
	for _, dp := range cm.DataPoints {
		idx := dp.Attributes["if_traffic_index"]
		byIndex[idx] = dp.Value
		byAttr[idx] = dp.Attributes["interface_description"]
	}

	assert.Equal(t, uint64(1000), byIndex["1"])
	assert.Equal(t, uint64(2000), byIndex["2"])
	assert.Equal(t, "eth0", byAttr["1"])
	assert.Equal(t, "eth1", byAttr["2"])
}

func TestCollectWithLookups(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.2.2.1.10.1":    uint64(1000),
		"1.3.6.1.2.1.2.2.1.10.2":    uint64(2000),
		"1.3.6.1.2.1.31.1.1.1.1.1":  "eth0",
		"1.3.6.1.2.1.31.1.1.1.1.2":  "eth1",
	})

	group := MetricGroupDef{
		Name: "if_traffic",
		Walk: "1.3.6.1.2.1.2.2.1",
		Metrics: []MetricDef{
			{
				OID:        "1.3.6.1.2.1.2.2.1.10",
				MetricName: "if_in_octets",
				Type:       "counter",
			},
		},
		Lookups: []LookupDef{
			{
				LookupOID:   "1.3.6.1.2.1.31.1.1.1.1",
				TargetLabel: "interface_name",
			},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results, 1)

	cm := results[0]
	require.Len(t, cm.DataPoints, 2)

	byIndex := map[string]string{}
	for _, dp := range cm.DataPoints {
		idx := dp.Attributes["if_traffic_index"]
		byIndex[idx] = dp.Attributes["interface_name"]
	}

	assert.Equal(t, "eth0", byIndex["1"])
	assert.Equal(t, "eth1", byIndex["2"])
}

func TestCollectScalarAttributes(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.5.0": "my-switch",
	})

	group := MetricGroupDef{
		Name: "system",
		ScalarAttributes: []AttributeDef{
			{
				OID:  "1.3.6.1.2.1.1.5.0",
				Name: "sys_name",
			},
		},
	}

	attrs, err := CollectScalarAttributes(mock, group)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"sys_name": "my-switch"}, attrs)
}

func TestCollectConnectionError(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetError(errors.New("connection refused"))

	group := MetricGroupDef{
		Name: "system",
		Metrics: []MetricDef{
			{
				OID:        "1.3.6.1.2.1.1.3.0",
				MetricName: "sys_uptime",
				Type:       "gauge",
			},
		},
	}

	_, err := Collect(mock, group)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}
