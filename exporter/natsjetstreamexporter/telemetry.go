package natsjetstreamexporter

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const scopeName = "go.olly.garden/grafts/exporter/natsjetstreamexporter"

// telemetry holds the exporter's self-observability instruments.
type telemetry struct {
	// publishErrors counts asynchronous publish failures, which are otherwise
	// invisible: an async publish returns before the server acknowledges, so
	// exporterhelper records it as sent. This counter is the only signal that
	// async data was lost.
	publishErrors metric.Int64Counter
}

func newTelemetry(set component.TelemetrySettings) (*telemetry, error) {
	m := set.MeterProvider.Meter(scopeName)
	t := &telemetry{}
	var err error
	if t.publishErrors, err = m.Int64Counter("natsjetstreamexporter.publish.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Asynchronous JetStream publish failures by error class.")); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *telemetry) recordPublishError(ctx context.Context, err error) {
	t.publishErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("error.type", classifyPublishError(err)),
	))
}

// classifyPublishError maps a publish error to a bounded error.type value so the
// counter stays low-cardinality.
func classifyPublishError(err error) string {
	switch {
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrDisconnected):
		return "connection"
	case errors.Is(err, jetstream.ErrNoStreamResponse):
		return "no_stream_response"
	case errors.Is(err, jetstream.ErrStreamNotFound):
		return "stream_not_found"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "other"
	}
}
