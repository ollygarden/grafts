package pgbouncerreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"go.olly.garden/grafts/internal/promcompat"
	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// componentType is the name of this receiver in configuration files.
const componentType = "pgbouncer"

// NewFactory creates a factory for the PgBouncer receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(componentType),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

// createDefaultConfig returns the defaults.
//
// The endpoint is PgBouncer's documented default listen port and the database
// is its admin database, so a local instance needs only credentials. `emit`
// defaults to the OTel shape alone: compatibility mode roughly doubles the
// series count, so it is something a user turns on for a migration rather than
// something they pay for by default.
func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:           "localhost:6432",
		Database:           "pgbouncer",
		TLS:                "disable",
		CollectionInterval: time.Minute,
		Timeout:            10 * time.Second,
		Emit:               promcompat.Emit{promcompat.ShapeOTel},
		Metrics:            telemetry.DefaultMetricsConfig(),
	}
}

func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("expected a pgbouncer receiver config, got %T", cfg)
	}
	return newReceiver(rCfg, &settings, next)
}
