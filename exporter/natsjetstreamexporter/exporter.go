package natsjetstreamexporter

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// propagator injects W3C trace context into outbound NATS message headers so the
// receiver can link its consume span to this exporter's send span.
var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// natsJetStreamExporter implements an exporter that publishes telemetry data
// to NATS JetStream streams using OTLP protobuf format.
type natsJetStreamExporter struct {
	config   *Config
	settings exporter.Settings
	logger   *zap.Logger
	tel      *telemetry

	// NATS resources
	conn *nats.Conn
	js   jetstream.JetStream
}

// newNatsJetStreamExporter creates a new NATS JetStream exporter.
func newNatsJetStreamExporter(cfg *Config, settings exporter.Settings) (*natsJetStreamExporter, error) {
	tel, err := newTelemetry(settings.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	return &natsJetStreamExporter{
		config:   cfg,
		settings: settings,
		logger:   settings.Logger,
		tel:      tel,
	}, nil
}

// Start establishes the NATS connection and JetStream context.
func (e *natsJetStreamExporter) Start(ctx context.Context, _ component.Host) error {
	opts := []nats.Option{
		nats.MaxReconnects(e.config.MaxReconnects),
		nats.ReconnectWait(e.config.ReconnectWait),
		nats.PingInterval(e.config.PingInterval),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			e.logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			e.logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			e.logger.Info("NATS connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			e.logger.Error("NATS error", zap.Error(err))
		}),
	}

	if e.config.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(e.config.CredentialsFile))
	}

	conn, err := nats.Connect(e.config.URL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	e.conn = conn

	// Async publishes complete after this call returns, so their failures cannot
	// be surfaced through the push return value. This handler records them to the
	// publish-errors counter so async data loss is observable.
	asyncErrHandler := jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, msg *nats.Msg, perr error) {
		e.tel.recordPublishError(context.Background(), perr)
		e.logger.Error("NATS async publish failed",
			zap.String("subject", msg.Subject),
			zap.Error(perr))
	})

	// Create JetStream context with optional domain
	var js jetstream.JetStream
	if e.config.Domain != "" {
		e.logger.Info("Using JetStream domain", zap.String("domain", e.config.Domain))
		js, err = jetstream.NewWithDomain(conn, e.config.Domain, asyncErrHandler)
	} else {
		js, err = jetstream.New(conn, asyncErrHandler)
	}
	if err != nil {
		e.conn.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	e.js = js

	// Verify stream exists (fail fast - don't create)
	_, err = js.Stream(ctx, e.config.Stream)
	if err != nil {
		e.conn.Close()
		return fmt.Errorf("failed to access stream %q: %w", e.config.Stream, err)
	}

	e.logger.Info("NATS JetStream exporter started",
		zap.String("url", e.config.URL),
		zap.String("stream", e.config.Stream),
		zap.Bool("async", e.config.PublishAsync))

	return nil
}

// Shutdown flushes pending publishes and closes the connection.
func (e *natsJetStreamExporter) Shutdown(ctx context.Context) error {
	if e.config.PublishAsync && e.js != nil {
		// Wait for pending async publishes to complete
		select {
		case <-e.js.PublishAsyncComplete():
			e.logger.Debug("All pending publishes completed")
		case <-ctx.Done():
			e.logger.Warn("Shutdown timeout waiting for pending publishes")
		}
	}

	if e.conn != nil {
		e.conn.Close()
	}

	e.logger.Info("NATS JetStream exporter shutdown complete")
	return nil
}

// pushTraces publishes traces to the configured subject.
func (e *natsJetStreamExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	if e.config.Subjects.Traces == "" {
		return nil // Subject not configured, skip
	}

	req := ptraceotlp.NewExportRequestFromTraces(td)
	data, err := req.MarshalProto()
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("failed to marshal traces: %w", err))
	}

	return e.publish(ctx, e.config.Subjects.Traces, data)
}

// pushMetrics publishes metrics to the configured subject.
func (e *natsJetStreamExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	if e.config.Subjects.Metrics == "" {
		return nil // Subject not configured, skip
	}

	req := pmetricotlp.NewExportRequestFromMetrics(md)
	data, err := req.MarshalProto()
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("failed to marshal metrics: %w", err))
	}

	return e.publish(ctx, e.config.Subjects.Metrics, data)
}

// pushLogs publishes logs to the configured subject.
func (e *natsJetStreamExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	if e.config.Subjects.Logs == "" {
		return nil // Subject not configured, skip
	}

	req := plogotlp.NewExportRequestFromLogs(ld)
	data, err := req.MarshalProto()
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("failed to marshal logs: %w", err))
	}

	return e.publish(ctx, e.config.Subjects.Logs, data)
}

// publish sends data to NATS JetStream (sync or async based on config). The
// outbound message carries injected W3C trace context so the receiver can link
// its consume span to the current export span.
func (e *natsJetStreamExporter) publish(ctx context.Context, subject string, data []byte) error {
	msg := nats.NewMsg(subject)
	msg.Data = data
	propagator.Inject(ctx, propagation.HeaderCarrier(msg.Header))

	opts := []jetstream.PublishOpt{
		jetstream.WithExpectStream(e.config.Stream),
	}

	if e.config.PublishAsync {
		return e.publishAsync(msg, opts)
	}
	return e.publishSync(ctx, msg, opts)
}

// publishSync performs synchronous publish with immediate acknowledgment.
func (e *natsJetStreamExporter) publishSync(
	ctx context.Context,
	msg *nats.Msg,
	opts []jetstream.PublishOpt,
) error {
	ack, err := e.js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return e.classifyError(err)
	}

	e.logger.Debug("Message published (sync)",
		zap.String("subject", msg.Subject),
		zap.String("stream", ack.Stream),
		zap.Uint64("seq", ack.Sequence))

	return nil
}

// publishAsync performs asynchronous publish for higher throughput. The publish
// completes after this returns, so per-message failures cannot be surfaced here;
// they are reported by the async error handler registered in Start (which feeds
// the publish-errors counter). Only a synchronous enqueue failure (e.g. the
// async buffer is full) is returned, so exporterhelper can retry it.
func (e *natsJetStreamExporter) publishAsync(
	msg *nats.Msg,
	opts []jetstream.PublishOpt,
) error {
	if _, err := e.js.PublishMsgAsync(msg, opts...); err != nil {
		return e.classifyError(err)
	}
	return nil
}

// classifyError determines if an error is retryable or permanent.
func (e *natsJetStreamExporter) classifyError(err error) error {
	if err == nil {
		return nil
	}

	// Connection errors are retryable
	if err == nats.ErrConnectionClosed || err == nats.ErrDisconnected {
		return fmt.Errorf("NATS connection error (retryable): %w", err)
	}

	// JetStream-specific errors
	switch err {
	case jetstream.ErrNoStreamResponse:
		// Stream not available - retryable
		return fmt.Errorf("stream not available (retryable): %w", err)
	case jetstream.ErrStreamNotFound:
		// Stream doesn't exist - permanent (configuration error)
		return consumererror.NewPermanent(fmt.Errorf("stream not found: %w", err))
	default:
		// Default to retryable for unknown errors
		return fmt.Errorf("publish error: %w", err)
	}
}
