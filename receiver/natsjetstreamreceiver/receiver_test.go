package natsjetstreamreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const testStream = "telemetry"

// createEmbeddedNATS initializes an embedded NATS JetStream server for testing.
// It creates a server with JetStream enabled and a stream for telemetry data.
func createEmbeddedNATS(tb testing.TB) jetstream.JetStream {
	ns, err := server.NewServer(&server.Options{
		JetStream: true,
		StoreDir:  tb.TempDir(),
		Port:      -1, // random available port
	})
	require.NoError(tb, err)

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		require.Fail(tb, "nats server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(tb, err)

	js, err := jetstream.New(nc)
	require.NoError(tb, err)

	// create the telemetry stream
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     testStream,
		Subjects: []string{"otlp.proto.>"},
	})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})

	return js
}

func TestReceiverIntegration_Traces(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create test traces data
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(100 * time.Millisecond)))

	// Serialize to OTLP proto
	req := ptraceotlp.NewExportRequestFromTraces(td)
	data, err := req.MarshalProto()
	require.NoError(t, err)

	// Publish the serialized proto to NATS
	tracesSubject := "otlp.proto.traces"
	_, err = js.Publish(ctx, tracesSubject, data)
	require.NoError(t, err)

	// Create and configure the receiver
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.ConsumerName = "test-receiver"
	cfg.Subjects.Traces = tracesSubject

	// Create a sink to capture received traces
	sink := &consumertest.TracesSink{}

	settings := receivertest.NewNopSettings(factory.Type())
	receiver, err := factory.CreateTraces(ctx, settings, cfg, sink)
	require.NoError(t, err)

	// Start the receiver
	err = receiver.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, receiver.Shutdown(ctx))
	})

	// Wait for the message to be consumed
	require.Eventually(t, func() bool {
		return sink.SpanCount() > 0
	}, 5*time.Second, 100*time.Millisecond, "expected to receive traces")

	// Verify received data
	require.Equal(t, 1, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	require.Len(t, receivedTraces, 1)

	receivedSpan := receivedTraces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, "test-span", receivedSpan.Name())

	serviceName, ok := receivedTraces[0].ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "test-service", serviceName.Str())
}

func TestReceiverIntegration_MultipleSignals(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the receiver
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.ConsumerName = "test-receiver-multi"

	settings := receivertest.NewNopSettings(factory.Type())

	// Create sinks for all signal types
	tracesSink := &consumertest.TracesSink{}
	metricsSink := &consumertest.MetricsSink{}
	logsSink := &consumertest.LogsSink{}

	// Create receivers for all signal types (they share the same underlying receiver)
	tracesReceiver, err := factory.CreateTraces(ctx, settings, cfg, tracesSink)
	require.NoError(t, err)

	metricsReceiver, err := factory.CreateMetrics(ctx, settings, cfg, metricsSink)
	require.NoError(t, err)

	logsReceiver, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

	// Start receivers (shared receiver starts on first Start call)
	err = tracesReceiver.Start(ctx, nil)
	require.NoError(t, err)
	err = metricsReceiver.Start(ctx, nil)
	require.NoError(t, err)
	err = logsReceiver.Start(ctx, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		// Shutdown in reverse order
		require.NoError(t, logsReceiver.Shutdown(ctx))
		require.NoError(t, metricsReceiver.Shutdown(ctx))
		require.NoError(t, tracesReceiver.Shutdown(ctx))
	})

	// Create and publish traces
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "multi-test")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("multi-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	tracesReq := ptraceotlp.NewExportRequestFromTraces(td)
	tracesData, err := tracesReq.MarshalProto()
	require.NoError(t, err)

	_, err = js.Publish(ctx, cfg.Subjects.Traces, tracesData)
	require.NoError(t, err)

	// Wait for traces to be consumed
	require.Eventually(t, func() bool {
		return tracesSink.SpanCount() > 0
	}, 5*time.Second, 100*time.Millisecond, "expected to receive traces")

	assert.Equal(t, 1, tracesSink.SpanCount())
}
