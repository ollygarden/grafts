package pgbouncerreceiver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// scopeName identifies this component's telemetry. It is also the prefix of the
// Prometheus compatibility scope, which is how users filter the compat set out.
const scopeName = "go.olly.garden/grafts/receiver/pgbouncerreceiver"

// pgbouncerReceiver polls PgBouncer's admin console on an interval.
type pgbouncerReceiver struct {
	cfg      *Config
	settings *receiver.Settings
	consumer consumer.Metrics

	scraper *scraper
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once

	// connect is a seam: lifecycle tests drive Start and Shutdown without a
	// PgBouncer to connect to.
	connect func(ctx context.Context, connString string) (client, error)
}

func newReceiver(cfg *Config, settings *receiver.Settings, next consumer.Metrics) (*pgbouncerReceiver, error) {
	self, err := telemetry.NewSelfTelemetry(settings.MeterProvider, scopeName)
	if err != nil {
		return nil, fmt.Errorf("creating self telemetry: %w", err)
	}

	version := settings.BuildInfo.Version
	return &pgbouncerReceiver{
		cfg:      cfg,
		settings: settings,
		consumer: next,
		done:     make(chan struct{}),
		connect: func(ctx context.Context, connString string) (client, error) {
			return newClient(ctx, connString)
		},
		scraper: &scraper{
			cfg: cfg,
			mb: telemetry.NewMetricsBuilder(
				cfg.Metrics, pcommon.NewTimestampFromTime(time.Now()), scopeName, version),
			self:         self,
			scopeName:    scopeName,
			scopeVersion: version,
		},
	}, nil
}

// Start connects to PgBouncer and begins collecting.
//
// The connection is opened here rather than lazily on the first scrape so that
// a wrong endpoint or a role missing from `stats_users` is a startup error the
// operator sees, not a metric that quietly never appears.
func (r *pgbouncerReceiver) Start(ctx context.Context, _ component.Host) error {
	c, err := r.connect(ctx, r.cfg.connString())
	if err != nil {
		return err
	}
	r.scraper.client = c

	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	go r.run(runCtx)
	return nil
}

// run drives collection until the context is cancelled.
func (r *pgbouncerReceiver) run(ctx context.Context) {
	defer close(r.done)

	ticker := time.NewTicker(r.cfg.CollectionInterval)
	defer ticker.Stop()

	r.collect(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.collect(ctx)
		}
	}
}

// collect runs one scrape and forwards whatever it produced.
func (r *pgbouncerReceiver) collect(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	md, err := r.scraper.scrape(ctx)
	if err != nil {
		// A partial scrape is normal: the failing commands are already counted
		// in self-telemetry, and what did come back is still worth sending.
		r.settings.Logger.Warn("pgbouncer scrape was incomplete", zap.Error(err))
	}
	if md.DataPointCount() == 0 {
		return
	}
	if err := r.consumer.ConsumeMetrics(ctx, md); err != nil {
		r.settings.Logger.Error("forwarding pgbouncer metrics failed", zap.Error(err))
	}
}

// Shutdown stops collection and closes the admin connection.
func (r *pgbouncerReceiver) Shutdown(ctx context.Context) error {
	var err error
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
			select {
			case <-r.done:
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
		if r.scraper.client != nil {
			err = r.scraper.client.Close(ctx)
		}
	})
	return err
}

// host returns the host half of a host:port endpoint, or the whole string when
// it carries no port.
func host(endpoint string) string {
	h, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint
	}
	return h
}

// port returns the port half of a host:port endpoint, or zero.
func port(endpoint string) int64 {
	_, p, err := net.SplitHostPort(endpoint)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
