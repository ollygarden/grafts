package natsjetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// fetchErrorBackoff throttles the consume loop after a non-terminal iterator
// error so a persistent failure cannot spin a tight, CPU-burning retry loop.
const fetchErrorBackoff = time.Second

// natsJetStreamReceiver implements a receiver that consumes telemetry data
// from NATS JetStream streams using pull-based consumers.
type natsJetStreamReceiver struct {
	config   *Config
	settings *receiver.Settings

	// Signal consumers (nil if not configured in pipeline)
	nextTraces  consumer.Traces
	nextMetrics consumer.Metrics
	nextLogs    consumer.Logs

	// NATS resources - one JetStream consumer per signal type
	conn        *nats.Conn
	js          jetstream.JetStream
	tracesIter  jetstream.MessagesContext
	metricsIter jetstream.MessagesContext
	logsIter    jetstream.MessagesContext

	// Observability
	obsrecv *receiverhelper.ObsReport
	tel     *telemetry

	// Lifecycle management
	cancel     context.CancelFunc
	shutdownWG sync.WaitGroup
}

// newNatsJetStreamReceiver creates a new NATS JetStream receiver.
func newNatsJetStreamReceiver(cfg *Config, settings *receiver.Settings) (*natsJetStreamReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "nats",
		ReceiverCreateSettings: *settings,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create obsreport: %w", err)
	}

	tel, err := newTelemetry(*settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry: %w", err)
	}

	return &natsJetStreamReceiver{
		config:   cfg,
		settings: settings,
		obsrecv:  obsrecv,
		tel:      tel,
	}, nil
}

// registerTracesConsumer registers a traces consumer.
func (r *natsJetStreamReceiver) registerTracesConsumer(tc consumer.Traces) {
	r.nextTraces = tc
}

// registerMetricsConsumer registers a metrics consumer.
func (r *natsJetStreamReceiver) registerMetricsConsumer(mc consumer.Metrics) {
	r.nextMetrics = mc
}

// registerLogsConsumer registers a logs consumer.
func (r *natsJetStreamReceiver) registerLogsConsumer(lc consumer.Logs) {
	r.nextLogs = lc
}

// Start begins consuming messages from NATS JetStream.
func (r *natsJetStreamReceiver) Start(ctx context.Context, _ component.Host) error {
	// Connect to NATS with resilience options
	opts := []nats.Option{
		nats.MaxReconnects(r.config.MaxReconnects),
		nats.ReconnectWait(r.config.ReconnectWait),
		nats.PingInterval(r.config.PingInterval),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			r.settings.Logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			r.settings.Logger.Info("NATS reconnected",
				zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			r.settings.Logger.Info("NATS connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			r.settings.Logger.Error("NATS error", zap.Error(err))
		}),
	}

	if r.config.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(r.config.CredentialsFile))
	}

	conn, err := nats.Connect(r.config.URL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	r.conn = conn

	// Create JetStream context with optional domain
	var js jetstream.JetStream
	if r.config.Domain != "" {
		r.settings.Logger.Info("Using JetStream domain", zap.String("domain", r.config.Domain))
		js, err = jetstream.NewWithDomain(conn, r.config.Domain)
	} else {
		js, err = jetstream.New(conn)
	}
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	r.js = js

	// Access the stream (fail if it doesn't exist)
	stream, err := js.Stream(ctx, r.config.Stream)
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to access stream %q: %w", r.config.Stream, err)
	}

	// Phase 1: Create all consumers and iterators first (before starting any message loops)
	// This ensures we don't have a partial state if one consumer fails to create
	if r.nextTraces != nil {
		if err := r.createSignalConsumer(ctx, stream, "traces", r.config.Subjects.Traces); err != nil {
			r.conn.Close()
			return err
		}
	}

	if r.nextMetrics != nil {
		if err := r.createSignalConsumer(ctx, stream, "metrics", r.config.Subjects.Metrics); err != nil {
			r.conn.Close()
			return err
		}
	}

	if r.nextLogs != nil {
		if err := r.createSignalConsumer(ctx, stream, "logs", r.config.Subjects.Logs); err != nil {
			r.conn.Close()
			return err
		}
	}

	// Phase 2: All consumers created successfully - now start message loops
	loopCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if r.tracesIter != nil {
		r.shutdownWG.Add(1)
		go r.signalLoop(loopCtx, "traces", r.tracesIter)
	}

	if r.metricsIter != nil {
		r.shutdownWG.Add(1)
		go r.signalLoop(loopCtx, "metrics", r.metricsIter)
	}

	if r.logsIter != nil {
		r.shutdownWG.Add(1)
		go r.signalLoop(loopCtx, "logs", r.logsIter)
	}

	r.settings.Logger.Info("NATS JetStream receiver started",
		zap.String("url", r.config.URL),
		zap.String("stream", r.config.Stream),
		zap.Bool("traces", r.nextTraces != nil),
		zap.Bool("metrics", r.nextMetrics != nil),
		zap.Bool("logs", r.nextLogs != nil))

	return nil
}

// createSignalConsumer creates a JetStream consumer and iterator for a specific signal type.
// The message processing loop is started separately in Phase 2 of Start().
func (r *natsJetStreamReceiver) createSignalConsumer(
	ctx context.Context,
	stream jetstream.Stream,
	signal string,
	subject string,
) error {
	// Create unique consumer name for this signal
	consumerName := fmt.Sprintf("%s-%s", r.config.ConsumerName, signal)

	// Create or update the consumer
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		DeliverGroup:  r.config.ConsumerGroup,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       r.config.AckWait,
		MaxDeliver:    r.config.MaxDeliver,
		MaxAckPending: r.config.MaxAckPending,
	})
	if err != nil {
		return fmt.Errorf("failed to create %s consumer: %w", signal, err)
	}

	// Create message iterator
	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("failed to create %s iterator: %w", signal, err)
	}

	// Store iterator for later use
	switch signal {
	case "traces":
		r.tracesIter = iter
	case "metrics":
		r.metricsIter = iter
	case "logs":
		r.logsIter = iter
	}

	r.settings.Logger.Info("Signal consumer created",
		zap.String("signal", signal),
		zap.String("consumer", consumerName),
		zap.String("subject", subject))

	return nil
}

// signalLoop is the message processing loop for a specific signal type.
func (r *natsJetStreamReceiver) signalLoop(ctx context.Context, signal string, iter jetstream.MessagesContext) {
	defer r.shutdownWG.Done()

	for {
		// Check for shutdown
		select {
		case <-ctx.Done():
			r.settings.Logger.Info("Signal loop shutting down", zap.String("signal", signal))
			return
		default:
		}

		// Fetch next message
		msg, err := iter.Next()
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				r.settings.Logger.Info("Message iterator closed", zap.String("signal", signal))
				return
			}
			r.settings.Logger.Error("Failed to get next message",
				zap.String("signal", signal),
				zap.Error(err))
			r.tel.recordFetchError(ctx, signal)
			// Back off so a persistent error does not spin a tight retry loop.
			select {
			case <-ctx.Done():
				return
			case <-time.After(fetchErrorBackoff):
			}
			continue
		}

		// Process the message
		r.handleMessage(signal, msg)
	}
}

// handleMessage processes a single message for a specific signal type.
func (r *natsJetStreamReceiver) handleMessage(signal string, msg jetstream.Msg) {
	data := msg.Data()

	// Link to the producer's send span via context propagated in message headers,
	// using the collector's configured propagator (no-op if none is configured).
	msgCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Headers()))

	switch signal {
	case "traces":
		ctx := r.obsrecv.StartTracesOp(msgCtx)
		req := ptraceotlp.NewExportRequest()
		err := req.UnmarshalProto(data)
		if err != nil {
			r.settings.Logger.Error("Failed to unmarshal traces",
				zap.Error(err),
				zap.String("subject", msg.Subject()))
			r.obsrecv.EndTracesOp(ctx, "protobuf", 0, err)
			// Parse errors are permanent - terminate message
			if termErr := msg.Term(); termErr != nil {
				r.settings.Logger.Error("Failed to terminate message", zap.Error(termErr))
			}
			return
		}

		td := req.Traces()
		err = r.nextTraces.ConsumeTraces(ctx, td)
		r.obsrecv.EndTracesOp(ctx, "protobuf", td.SpanCount(), err)
		r.ackOrNak(msg, err)

	case "metrics":
		ctx := r.obsrecv.StartMetricsOp(msgCtx)
		req := pmetricotlp.NewExportRequest()
		err := req.UnmarshalProto(data)
		if err != nil {
			r.settings.Logger.Error("Failed to unmarshal metrics",
				zap.Error(err),
				zap.String("subject", msg.Subject()))
			r.obsrecv.EndMetricsOp(ctx, "protobuf", 0, err)
			// Parse errors are permanent - terminate message
			if termErr := msg.Term(); termErr != nil {
				r.settings.Logger.Error("Failed to terminate message", zap.Error(termErr))
			}
			return
		}

		md := req.Metrics()
		err = r.nextMetrics.ConsumeMetrics(ctx, md)
		r.obsrecv.EndMetricsOp(ctx, "protobuf", md.DataPointCount(), err)
		r.ackOrNak(msg, err)

	case "logs":
		ctx := r.obsrecv.StartLogsOp(msgCtx)
		req := plogotlp.NewExportRequest()
		err := req.UnmarshalProto(data)
		if err != nil {
			r.settings.Logger.Error("Failed to unmarshal logs",
				zap.Error(err),
				zap.String("subject", msg.Subject()))
			r.obsrecv.EndLogsOp(ctx, "protobuf", 0, err)
			// Parse errors are permanent - terminate message
			if termErr := msg.Term(); termErr != nil {
				r.settings.Logger.Error("Failed to terminate message", zap.Error(termErr))
			}
			return
		}

		ld := req.Logs()
		err = r.nextLogs.ConsumeLogs(ctx, ld)
		r.obsrecv.EndLogsOp(ctx, "protobuf", ld.LogRecordCount(), err)
		r.ackOrNak(msg, err)
	}
}

// ackOrNak acknowledges or negatively acknowledges a message based on the error.
func (r *natsJetStreamReceiver) ackOrNak(msg jetstream.Msg, err error) {
	if err == nil {
		if ackErr := msg.Ack(); ackErr != nil {
			r.settings.Logger.Error("Failed to ack message", zap.Error(ackErr))
		}
		return
	}

	// Determine if error is retryable
	if consumererror.IsPermanent(err) {
		// Permanent error - terminate message
		if termErr := msg.Term(); termErr != nil {
			r.settings.Logger.Error("Failed to terminate message", zap.Error(termErr))
		}
	} else {
		// Retryable error - nak with delay
		if nakErr := msg.NakWithDelay(5 * time.Second); nakErr != nil {
			r.settings.Logger.Error("Failed to nak message", zap.Error(nakErr))
		}
	}
}

// Shutdown gracefully shuts down the receiver.
func (r *natsJetStreamReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	// Drain iterators to process in-flight messages (Drain() doesn't return error)
	if r.tracesIter != nil {
		r.tracesIter.Drain()
		r.settings.Logger.Debug("Drained traces iterator")
	}
	if r.metricsIter != nil {
		r.metricsIter.Drain()
		r.settings.Logger.Debug("Drained metrics iterator")
	}
	if r.logsIter != nil {
		r.logsIter.Drain()
		r.settings.Logger.Debug("Drained logs iterator")
	}

	// Wait for all message loops to finish
	r.shutdownWG.Wait()

	// Close NATS connection
	if r.conn != nil {
		r.conn.Close()
	}

	r.settings.Logger.Info("NATS JetStream receiver shutdown complete")
	return nil
}
