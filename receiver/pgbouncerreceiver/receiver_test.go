package pgbouncerreceiver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// countingClient records how many scrapes ran and whether it was closed, so the
// lifecycle assertions do not depend on timing alone.
type countingClient struct {
	fakeClient
	queries atomic.Int64
	closed  atomic.Bool
}

func (c *countingClient) Query(ctx context.Context, cmd Command) ([]Row, error) {
	if cmd == CommandDatabases {
		c.queries.Add(1)
	}
	return c.fakeClient.Query(ctx, cmd)
}

func (c *countingClient) Close(context.Context) error {
	c.closed.Store(true)
	return nil
}

func newTestReceiver(t *testing.T, next *consumertest.MetricsSink, c client, connectErr error) *pgbouncerReceiver {
	t.Helper()

	cfg := validConfig()
	cfg.CollectionInterval = 20 * time.Millisecond
	cfg.Timeout = 10 * time.Millisecond

	r, err := newReceiver(cfg, ptr(receivertest.NewNopSettings(NewFactory().Type())), next)
	require.NoError(t, err)

	r.connect = func(context.Context, string) (client, error) {
		if connectErr != nil {
			return nil, connectErr
		}
		return c, nil
	}
	return r
}

func ptr[T any](v T) *T { return &v }

func TestReceiverCollectsAndShutsDown(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	c := &countingClient{fakeClient: fakeClient{rows: fixtureRows()}}
	r := newTestReceiver(t, sink, c, nil)

	require.NoError(t, r.Start(t.Context(), componenttest.NewNopHost()))

	// The first collection runs immediately rather than after one interval, so
	// a receiver with a long interval still proves itself at startup.
	require.Eventually(t, func() bool { return len(sink.AllMetrics()) > 0 }, time.Second, 5*time.Millisecond)

	require.NoError(t, r.Shutdown(t.Context()))
	assert.True(t, c.closed.Load(), "shutdown must close the admin connection")

	// Nothing keeps scraping after shutdown.
	after := c.queries.Load()
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, after, c.queries.Load())

	md := sink.AllMetrics()[0]
	assert.Positive(t, md.DataPointCount())
}

func TestReceiverStartFailsOnConnectError(t *testing.T) {
	r := newTestReceiver(t, new(consumertest.MetricsSink), nil, errors.New("no route to host"))

	// Connecting at startup rather than lazily is deliberate: a wrong endpoint
	// or a role missing from stats_users must be an error the operator sees,
	// not a metric that quietly never appears.
	err := r.Start(t.Context(), componenttest.NewNopHost())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no route to host")
}

func TestReceiverShutdownIsIdempotent(t *testing.T) {
	c := &countingClient{fakeClient: fakeClient{rows: fixtureRows()}}
	r := newTestReceiver(t, new(consumertest.MetricsSink), c, nil)

	require.NoError(t, r.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, r.Shutdown(t.Context()))
	assert.NoError(t, r.Shutdown(t.Context()))
}

func TestReceiverShutdownWithoutStart(t *testing.T) {
	r := newTestReceiver(t, new(consumertest.MetricsSink), nil, nil)

	assert.NoError(t, r.Shutdown(t.Context()))
}

func TestReceiverForwardsNothingWhenScrapeIsEmpty(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	c := &countingClient{fakeClient: fakeClient{
		errs: map[Command]error{
			CommandDatabases: errors.New("down"), CommandPools: errors.New("down"),
			CommandStats: errors.New("down"), CommandLists: errors.New("down"),
			CommandConfig: errors.New("down"), CommandClients: errors.New("down"),
			CommandVersion: errors.New("down"),
		},
	}}
	r := newTestReceiver(t, sink, c, nil)

	require.NoError(t, r.Start(t.Context(), componenttest.NewNopHost()))
	time.Sleep(80 * time.Millisecond)
	require.NoError(t, r.Shutdown(t.Context()))

	// A totally failed scrape sends nothing rather than an empty envelope the
	// rest of the pipeline has to walk.
	var points int
	for _, md := range sink.AllMetrics() {
		points += md.DataPointCount()
	}
	assert.Zero(t, points)
}

func TestEndpointParsing(t *testing.T) {
	assert.Equal(t, "pgbouncer.example", host("pgbouncer.example:6432"))
	assert.Equal(t, int64(6432), port("pgbouncer.example:6432"))

	// An endpoint with no port keeps its host rather than becoming empty.
	assert.Equal(t, "pgbouncer.example", host("pgbouncer.example"))
	assert.Equal(t, int64(0), port("pgbouncer.example"))
}
