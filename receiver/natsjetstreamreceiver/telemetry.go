package natsjetstreamreceiver

import (
	"context"

	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const scopeName = "go.olly.garden/grafts/receiver/natsjetstreamreceiver"

// telemetry holds the receiver's self-observability instruments.
type telemetry struct {
	// fetchErrors counts failures pulling the next message from a JetStream
	// iterator (excluding the expected closed-iterator case). A rising count
	// signals a stuck or backing-off consume loop.
	fetchErrors metric.Int64Counter
}

func newTelemetry(set receiver.Settings) (*telemetry, error) {
	m := set.MeterProvider.Meter(scopeName)
	t := &telemetry{}
	var err error
	if t.fetchErrors, err = m.Int64Counter("natsjetstreamreceiver.fetch.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Failures fetching the next message from a JetStream iterator, by signal.")); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *telemetry) recordFetchError(ctx context.Context, signal string) {
	t.fetchErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", signal)))
}
