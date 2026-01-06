package natsjetstreamexporter

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

const testStream = "telemetry"

// createEmbeddedNATS initializes an embedded NATS JetStream server for testing.
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

	// Create the telemetry stream
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

func TestExporterIntegration_Traces(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the exporter
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = false // Use sync for easier testing

	settings := exportertest.NewNopSettings(factory.Type())
	exporter, err := factory.CreateTraces(ctx, settings, cfg)
	require.NoError(t, err)

	// Start the exporter
	err = exporter.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, exporter.Shutdown(ctx))
	})

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

	// Export traces
	err = exporter.ConsumeTraces(ctx, td)
	require.NoError(t, err)

	// Create a consumer to verify the message was published
	cons, err := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Name:          "test-consumer",
		FilterSubject: cfg.Subjects.Traces,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Fetch the message
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)

	var receivedMsg jetstream.Msg
	for msg := range msgs.Messages() {
		receivedMsg = msg
	}
	require.NotNil(t, receivedMsg)

	// Verify the message content
	req := ptraceotlp.NewExportRequest()
	err = req.UnmarshalProto(receivedMsg.Data())
	require.NoError(t, err)

	receivedTraces := req.Traces()
	assert.Equal(t, 1, receivedTraces.SpanCount())

	receivedSpan := receivedTraces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, "test-span", receivedSpan.Name())

	serviceName, ok := receivedTraces.ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "test-service", serviceName.Str())
}

func TestExporterIntegration_Metrics(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the exporter
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = false

	settings := exportertest.NewNopSettings(factory.Type())
	exporter, err := factory.CreateMetrics(ctx, settings, cfg)
	require.NoError(t, err)

	// Start the exporter
	err = exporter.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, exporter.Shutdown(ctx))
	})

	// Create test metrics data
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(42)

	// Export metrics
	err = exporter.ConsumeMetrics(ctx, md)
	require.NoError(t, err)

	// Create a consumer to verify the message was published
	cons, err := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Name:          "test-metrics-consumer",
		FilterSubject: cfg.Subjects.Metrics,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Fetch the message
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)

	var receivedMsg jetstream.Msg
	for msg := range msgs.Messages() {
		receivedMsg = msg
	}
	require.NotNil(t, receivedMsg)

	// Verify the message content
	req := pmetricotlp.NewExportRequest()
	err = req.UnmarshalProto(receivedMsg.Data())
	require.NoError(t, err)

	receivedMetrics := req.Metrics()
	assert.Equal(t, 1, receivedMetrics.DataPointCount())

	receivedMetric := receivedMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, "test.metric", receivedMetric.Name())
}

func TestExporterIntegration_Logs(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the exporter
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = false

	settings := exportertest.NewNopSettings(factory.Type())
	exporter, err := factory.CreateLogs(ctx, settings, cfg)
	require.NoError(t, err)

	// Start the exporter
	err = exporter.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, exporter.Shutdown(ctx))
	})

	// Create test logs data
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("test log message")
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Export logs
	err = exporter.ConsumeLogs(ctx, ld)
	require.NoError(t, err)

	// Create a consumer to verify the message was published
	cons, err := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Name:          "test-logs-consumer",
		FilterSubject: cfg.Subjects.Logs,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Fetch the message
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)

	var receivedMsg jetstream.Msg
	for msg := range msgs.Messages() {
		receivedMsg = msg
	}
	require.NotNil(t, receivedMsg)

	// Verify the message content
	req := plogotlp.NewExportRequest()
	err = req.UnmarshalProto(receivedMsg.Data())
	require.NoError(t, err)

	receivedLogs := req.Logs()
	assert.Equal(t, 1, receivedLogs.LogRecordCount())

	receivedLog := receivedLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "test log message", receivedLog.Body().Str())
}

func TestExporterIntegration_AsyncPublish(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the exporter with async publishing
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = true // Enable async publishing

	settings := exportertest.NewNopSettings(factory.Type())
	exporter, err := factory.CreateTraces(ctx, settings, cfg)
	require.NoError(t, err)

	// Start the exporter
	err = exporter.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, exporter.Shutdown(ctx))
	})

	// Create and export multiple traces
	for i := 0; i < 5; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("async-span")
		span.SetTraceID(pcommon.TraceID([16]byte{byte(i), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
		span.SetSpanID(pcommon.SpanID([8]byte{byte(i), 2, 3, 4, 5, 6, 7, 8}))

		err = exporter.ConsumeTraces(ctx, td)
		require.NoError(t, err)
	}

	// Wait a bit for async publishes to complete
	time.Sleep(500 * time.Millisecond)

	// Create a consumer to verify messages were published
	cons, err := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Name:          "test-async-consumer",
		FilterSubject: cfg.Subjects.Traces,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	// Fetch all messages
	msgs, err := cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)

	count := 0
	for range msgs.Messages() {
		count++
	}
	assert.Equal(t, 5, count, "expected 5 messages to be published")
}

func TestExporterStart_StreamNotFound(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	// Create and configure the exporter with a non-existent stream
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = "non-existent-stream"

	settings := exportertest.NewNopSettings(factory.Type())
	exporter, err := factory.CreateTraces(ctx, settings, cfg)
	require.NoError(t, err)

	// Start should fail because stream doesn't exist
	err = exporter.Start(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to access stream")
}
