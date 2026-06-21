package natsjetstreamreceiver

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver/receivertest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// traceCapturingConsumer records the trace ID present on the consume context.
type traceCapturingConsumer struct {
	*consumertest.TracesSink
	mu      sync.Mutex
	traceID trace.TraceID
}

func (c *traceCapturingConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	c.traceID = trace.SpanContextFromContext(ctx).TraceID()
	c.mu.Unlock()
	return c.TracesSink.ConsumeTraces(ctx, td)
}

func (c *traceCapturingConsumer) capturedTraceID() trace.TraceID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.traceID
}

// TestReceiverExtractsTraceContext verifies the receiver extracts W3C trace
// context from inbound message headers, so its consume span links to the
// producer's trace.
func TestReceiverExtractsTraceContext(t *testing.T) {
	js := createEmbeddedNATS(t)
	ctx := context.Background()

	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("producer-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	data, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	require.NoError(t, err)

	wantTraceID := trace.TraceID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	traceparent := fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(wantTraceID[:]), hex.EncodeToString(spanID[:]))

	subject := "otlp.proto.traces"
	_, err = js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Traceparent": []string{traceparent}},
	})
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URL = js.Conn().ConnectedUrl()
	cfg.Stream = testStream
	cfg.ConsumerName = "prop-receiver"
	cfg.Subjects.Traces = subject

	consumer := &traceCapturingConsumer{TracesSink: &consumertest.TracesSink{}}

	settings := receivertest.NewNopSettings(factory.Type())
	// A real tracer provider is required so the receiver's consume span is
	// recording and retains the extracted parent trace ID.
	settings.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))

	rcv, err := factory.CreateTraces(ctx, settings, cfg, consumer)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, nil))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	require.Eventually(t, func() bool {
		return consumer.SpanCount() > 0
	}, 5*time.Second, 100*time.Millisecond, "expected to receive traces")

	assert.Equal(t, wantTraceID, consumer.capturedTraceID(), "consume context should carry the producer trace ID")
}
