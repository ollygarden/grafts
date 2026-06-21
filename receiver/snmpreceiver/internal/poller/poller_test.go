package poller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zaptest"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
)

// newTestObsReport builds an ObsReport backed by Nop telemetry for tests.
func newTestObsReport(t *testing.T) *receiverhelper.ObsReport {
	t.Helper()
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.MustNewID("snmp"),
		Transport:              "snmp",
		ReceiverCreateSettings: receivertest.NewNopSettings(component.MustNewType("snmp")),
	})
	require.NoError(t, err)
	return obsrecv
}

// newObsReportWithTelemetry builds an ObsReport whose metrics are captured by
// the returned Telemetry, so tests can assert on receiver-level counters.
func newObsReportWithTelemetry(t *testing.T) (*receiverhelper.ObsReport, *componenttest.Telemetry) {
	t.Helper()
	tel := componenttest.NewTelemetry()
	set := receivertest.NewNopSettings(component.MustNewType("snmp"))
	set.TelemetrySettings = tel.NewTelemetrySettings()
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.MustNewID("snmp"),
		Transport:              "snmp",
		ReceiverCreateSettings: set,
	})
	require.NoError(t, err)
	return obsrecv, tel
}

func TestPollerStartStop(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(123456),
	})

	target := TargetDef{
		Host: "192.0.2.1",
		Port: 161,
		Conn: mock,
		MetricGroups: []MetricGroupDef{
			{
				Name: "system",
				Metrics: []MetricDef{
					{
						OID:        "1.3.6.1.2.1.1.3.0",
						MetricName: "sys_uptime",
						Type:       "gauge",
						Unit:       "ms",
					},
				},
			},
		},
		ResourceAttrs: map[string]string{},
	}

	sink := new(consumertest.MetricsSink)
	logger := zaptest.NewLogger(t)
	p := New(logger, []TargetDef{target}, 100*time.Millisecond, sink, newTestObsReport(t))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()

	// Wait for at least 1 metric batch in the sink.
	require.Eventually(t, func() bool {
		return sink.DataPointCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	// Verify got metrics with correct resource attributes.
	allMetrics := sink.AllMetrics()
	require.NotEmpty(t, allMetrics)

	rm := allMetrics[0].ResourceMetrics().At(0)
	res := rm.Resource()
	hostVal, ok := res.Attributes().Get("snmp.host")
	require.True(t, ok)
	assert.Equal(t, "192.0.2.1", hostVal.Str())

	portVal, ok := res.Attributes().Get("snmp.port")
	require.True(t, ok)
	assert.Equal(t, int64(161), portVal.Int())
}

func TestPollerTargetError(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetError(errors.New("connection refused"))

	target := TargetDef{
		Host: "192.0.2.2",
		Port: 161,
		Conn: mock,
		MetricGroups: []MetricGroupDef{
			{
				Name: "system",
				Metrics: []MetricDef{
					{
						OID:        "1.3.6.1.2.1.1.3.0",
						MetricName: "sys_uptime",
						Type:       "gauge",
					},
				},
			},
		},
		ResourceAttrs: map[string]string{},
	}

	sink := new(consumertest.MetricsSink)
	logger := zaptest.NewLogger(t)
	p := New(logger, []TargetDef{target}, 100*time.Millisecond, sink, newTestObsReport(t))

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()

	<-done

	// Errors are logged, not propagated: sink should be empty.
	assert.Equal(t, 0, sink.DataPointCount())
}

func TestPollerRecordsAcceptedPoints(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(123456),
	})

	target := TargetDef{
		Host: "192.0.2.1",
		Port: 161,
		Conn: mock,
		MetricGroups: []MetricGroupDef{
			{
				Name: "system",
				Metrics: []MetricDef{
					{OID: "1.3.6.1.2.1.1.3.0", MetricName: "sys_uptime", Type: "gauge", Unit: "ms"},
				},
			},
		},
		ResourceAttrs: map[string]string{},
	}

	sink := new(consumertest.MetricsSink)
	obsrecv, tel := newObsReportWithTelemetry(t)
	logger := zaptest.NewLogger(t)
	p := New(logger, []TargetDef{target}, 100*time.Millisecond, sink, obsrecv)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return sink.DataPointCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	m, err := tel.GetMetric("otelcol_receiver_accepted_metric_points")
	require.NoError(t, err)
	sum := m.Data.(metricdata.Sum[int64])
	require.NotEmpty(t, sum.DataPoints)
	assert.Positive(t, sum.DataPoints[0].Value, "expected accepted metric points to be recorded")
}
