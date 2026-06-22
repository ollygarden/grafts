package natsjetstreamexporter

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func newTestTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	return td
}

// TestPublishInjectsTraceContext verifies the exporter injects W3C trace context
// into outbound NATS message headers, so a downstream receiver can link to it.
func TestPublishInjectsTraceContext(t *testing.T) {
	// Mirror an operator-configured collector propagator; the global default is a no-op.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	js := createEmbeddedNATS(t)
	ctx := context.Background()

	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = false

	exp, err := newNatsJetStreamExporter(cfg, exportertest.NewNopSettings(NewFactory().Type()))
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, nil))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	traceID := trace.TraceID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	pubCtx := trace.ContextWithSpanContext(ctx, sc)

	require.NoError(t, exp.pushTraces(pubCtx, newTestTraces()))

	cons, err := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{
		Name:          "prop-consumer",
		FilterSubject: cfg.Subjects.Traces,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	require.NoError(t, err)

	var got jetstream.Msg
	for msg := range msgs.Messages() {
		got = msg
	}
	require.NotNil(t, got)

	// Extract via the same carrier the receiver uses and confirm the trace ID round-trips.
	extracted := otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(got.Headers()))
	assert.Equal(t, traceID, trace.SpanContextFromContext(extracted).TraceID())
}

// TestAsyncPublishErrorRecorded verifies that an async publish failure increments
// the publish-errors counter, so otherwise-silent async data loss is observable.
func TestAsyncPublishErrorRecorded(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.PublishAsync = true
	// A subject not bound to the stream: the server rejects the publish, and the
	// failure can only surface through the async error handler.
	cfg.Subjects.Traces = "unbound.subject"

	tel := componenttest.NewTelemetry()
	set := exportertest.NewNopSettings(NewFactory().Type())
	set.TelemetrySettings = tel.NewTelemetrySettings()

	exp, err := newNatsJetStreamExporter(cfg, set)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, nil))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	require.NoError(t, exp.pushTraces(ctx, newTestTraces()))

	require.Eventually(t, func() bool {
		m, err := tel.GetMetric("natsjetstreamexporter.publish.errors")
		if err != nil {
			return false
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		return ok && len(sum.DataPoints) > 0 && sum.DataPoints[0].Value > 0
	}, 3*time.Second, 50*time.Millisecond, "expected publish.errors counter to record the async failure")
}
