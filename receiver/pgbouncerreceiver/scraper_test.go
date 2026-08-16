package pgbouncerreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"

	"go.olly.garden/grafts/internal/promcompat"
	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// fakeClient answers admin commands from canned rows, so every decision the
// scraper makes -- the alias join, the state mapping, the aggregation -- is
// testable without a container.
type fakeClient struct {
	rows map[Command][]Row
	errs map[Command]error
}

func (c *fakeClient) Query(_ context.Context, cmd Command) ([]Row, error) {
	if err, ok := c.errs[cmd]; ok {
		return nil, err
	}
	return c.rows[cmd], nil
}

func (c *fakeClient) Close(context.Context) error { return nil }

// fixtureRows mirrors the shape of a real admin console, including the alias
// `seconddb` fronting the backend database `postgres` -- the case that makes
// the SHOW DATABASES join necessary.
func fixtureRows() map[Command][]Row {
	return map[Command][]Row{
		CommandDatabases: {
			{
				"name": "testdb", "database": "testdb", "host": "postgres", "port": "5432",
				"pool_size": "20", "min_pool_size": "3", "reserve_pool_size": "5",
				"max_connections": "100", "current_connections": "4",
				"paused": "0", "disabled": "0", "force_user": "", "pool_mode": "",
			},
			{
				"name": "seconddb", "database": "postgres", "host": "postgres", "port": "5432",
				"pool_size": "10", "min_pool_size": "0", "reserve_pool_size": "0",
				"max_connections": "50", "current_connections": "1",
				"paused": "1", "disabled": "0", "force_user": "admin", "pool_mode": "statement",
			},
		},
		CommandPools: {
			{
				"database": "seconddb", "user": "testuser",
				"cl_active": "3", "cl_waiting": "2",
				"cl_active_cancel_req": "0", "cl_waiting_cancel_req": "0",
				"sv_active": "1", "sv_idle": "4", "sv_used": "2", "sv_tested": "0",
				"sv_login": "0", "sv_active_cancel": "0", "sv_being_canceled": "0",
				"maxwait": "3", "maxwait_us": "500000",
			},
		},
		CommandStats: {
			{
				"database":          "seconddb",
				"total_query_count": "100", "total_xact_count": "40",
				"total_server_assignment_count": "7",
				"total_query_time":              "2500000", "total_xact_time": "4000000",
				"total_wait_time": "1500000",
				"total_received":  "8192", "total_sent": "4096",
				"total_client_parse_count": "9", "total_server_parse_count": "3",
				"total_bind_count": "12",
			},
		},
		CommandLists: {
			{"list": "databases", "items": "2"},
			{"list": "users", "items": "3"},
			{"list": "pools", "items": "1"},
			{"list": "free_clients", "items": "40"},
			{"list": "used_clients", "items": "5"},
			{"list": "login_clients", "items": "0"},
			{"list": "free_servers", "items": "8"},
			{"list": "used_servers", "items": "2"},
			{"list": "dns_names", "items": "1"},
			{"list": "dns_zones", "items": "0"},
			{"list": "dns_pending", "items": "0"},
		},
		CommandConfig: {
			{"key": "max_client_conn", "value": "100"},
			{"key": "max_user_connections", "value": "50"},
			{"key": "pool_mode", "value": "transaction"},
		},
		CommandClients: {
			{"database": "seconddb", "user": "testuser", "state": "active", "application_name": "psql"},
			{"database": "seconddb", "user": "testuser", "state": "active", "application_name": "app"},
			{"database": "seconddb", "user": "testuser", "state": "idle", "application_name": "psql"},
		},
		CommandVersion: {{"version": "PgBouncer 1.25.2"}},
	}
}

func newTestScraper(t *testing.T, client client, emit ...promcompat.Shape) *scraper {
	t.Helper()

	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)
	cfg.Endpoint = "pgbouncer.example:6432"
	if len(emit) > 0 {
		cfg.Emit = emit
	}

	self, err := telemetry.NewSelfTelemetry(noopmetric.NewMeterProvider(), scopeName)
	require.NoError(t, err)
	compat, err := promcompat.NewTable(telemetry.CompatTable, scopeName, "test")
	require.NoError(t, err)

	return &scraper{
		cfg:    cfg,
		client: client,
		mb:     telemetry.NewMetricsBuilder(cfg.Metrics, pcommon.NewTimestampFromTime(time.Now()), scopeName, "test"),
		self:   self,
		compat: compat,
		host:   host(cfg.Endpoint),
		port:   port(cfg.Endpoint),
	}
}

// index flattens metrics into name -> datapoint attribute maps and values.
func index(t *testing.T, md pmetric.Metrics, scope string) map[string][]pmetric.NumberDataPoint {
	t.Helper()

	out := map[string][]pmetric.NumberDataPoint{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			if sms.At(j).Scope().Name() != scope {
				continue
			}
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				var dps pmetric.NumberDataPointSlice
				switch m.Type() {
				case pmetric.MetricTypeSum:
					dps = m.Sum().DataPoints()
				case pmetric.MetricTypeGauge:
					dps = m.Gauge().DataPoints()
				default:
					continue
				}
				for l := 0; l < dps.Len(); l++ {
					out[m.Name()] = append(out[m.Name()], dps.At(l))
				}
			}
		}
	}
	return out
}

func findDP(dps []pmetric.NumberDataPoint, key, value string) (pmetric.NumberDataPoint, bool) {
	for _, dp := range dps {
		if v, ok := dp.Attributes().Get(key); ok && v.AsString() == value {
			return dp, true
		}
	}
	return pmetric.NumberDataPoint{}, false
}

func TestScrapeResolvesAliasToBackendDatabase(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	metrics := index(t, md, scopeName)
	dps := metrics["pgbouncer.query.count"]
	require.Len(t, dps, 1)

	// SHOW STATS reports the alias `seconddb`; only SHOW DATABASES knows it
	// fronts the backend database `postgres`. Without the join, db.namespace
	// would be the alias and would not identify anything on the server.
	ns, ok := dps[0].Attributes().Get("db.namespace")
	require.True(t, ok)
	assert.Equal(t, "postgres", ns.AsString())

	alias, ok := dps[0].Attributes().Get("pgbouncer.database.alias")
	require.True(t, ok)
	assert.Equal(t, "seconddb", alias.AsString())
}

func TestScrapeSeparatesPoolerSides(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	metrics := index(t, md, scopeName)

	// The server side takes the released convention because PgBouncer is
	// PostgreSQL's client there.
	server := metrics["db.client.connection.count"]
	require.Len(t, server, len(serverStates))

	// sv_active is what the convention calls `used`.
	used, ok := findDP(server, "db.client.connection.state", telemetry.AttributeDBClientConnectionStateUsed)
	require.True(t, ok)
	assert.Equal(t, int64(1), used.IntValue())

	// sv_used is NOT `used`: it means idle beyond server_check_delay. Reporting
	// it as `used` would double-count against sv_active and invert the metric.
	pending, ok := findDP(server, "db.client.connection.state", StateUsedPendingCheck)
	require.True(t, ok)
	assert.Equal(t, int64(2), pending.IntValue())

	idle, ok := findDP(server, "db.client.connection.state", telemetry.AttributeDBClientConnectionStateIdle)
	require.True(t, ok)
	assert.Equal(t, int64(4), idle.IntValue())

	// The client side is a local namespace; collapsing it into the convention
	// would make pool saturation and client queueing indistinguishable.
	client := metrics["pgbouncer.client.connection.count"]
	require.Len(t, client, len(clientStates))
	waiting, ok := findDP(client, "pgbouncer.client.connection.state", telemetry.AttributePgbouncerClientConnectionStateWaiting)
	require.True(t, ok)
	assert.Equal(t, int64(2), waiting.IntValue())
}

func TestScrapeReportsWaitingBothWays(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	metrics := index(t, md, scopeName)

	// cl_waiting is deliberately reported twice: as a client-side state, and as
	// the convention's view of server-pool saturation.
	pending := metrics["db.client.connection.pending_requests"]
	require.Len(t, pending, 1)
	assert.Equal(t, int64(2), pending[0].IntValue())

	client := metrics["pgbouncer.client.connection.count"]
	waiting, ok := findDP(client, "pgbouncer.client.connection.state", telemetry.AttributePgbouncerClientConnectionStateWaiting)
	require.True(t, ok)
	assert.Equal(t, pending[0].IntValue(), waiting.IntValue())
}

func TestScrapeConvertsMicrosecondsToSeconds(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	metrics := index(t, md, scopeName)

	// PgBouncer reports total_query_time in microseconds; the unit is seconds.
	require.Len(t, metrics["pgbouncer.query.time"], 1)
	assert.InDelta(t, 2.5, metrics["pgbouncer.query.time"][0].DoubleValue(), 1e-9)

	// maxwait_us is the sub-second remainder of maxwait, not a separate value.
	require.Len(t, metrics["pgbouncer.client.wait.max"], 1)
	assert.InDelta(t, 3.5, metrics["pgbouncer.client.wait.max"][0].DoubleValue(), 1e-9)
}

func TestScrapeDropsApplicationNameFromOTelShape(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	metrics := index(t, md, scopeName)

	// Three client rows across two application names collapse to two
	// datapoints, by state. application_name is client-supplied and therefore
	// unbounded, so it is not a dimension here.
	dps := metrics["pgbouncer.client.connection.detail.count"]
	require.Len(t, dps, 2)
	for _, dp := range dps {
		_, ok := dp.Attributes().Get("application_name")
		assert.False(t, ok, "application_name must not reach the OTel shape")
	}

	active, ok := findDP(dps, "pgbouncer.client.state", "active")
	require.True(t, ok)
	assert.Equal(t, int64(2), active.IntValue())
}

func TestScrapeSetsResourceAttributes(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	require.Positive(t, md.ResourceMetrics().Len())

	attrs := md.ResourceMetrics().At(0).Resource().Attributes()
	system, ok := attrs.Get("db.system.name")
	require.True(t, ok)
	assert.Equal(t, "postgresql", system.AsString())

	address, ok := attrs.Get("server.address")
	require.True(t, ok)
	assert.Equal(t, "pgbouncer.example", address.AsString())

	port, ok := attrs.Get("server.port")
	require.True(t, ok)
	assert.Equal(t, int64(6432), port.Int())
}

func TestScrapeIsPartialWhenACommandFails(t *testing.T) {
	rows := fixtureRows()
	client := &fakeClient{
		rows: rows,
		errs: map[Command]error{CommandPools: errors.New("connection reset")},
	}
	s := newTestScraper(t, client)

	md, err := s.scrape(t.Context())

	// The failure is reported, but everything else still is too -- which is the
	// behaviour users depend on from the upstream exporter.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")

	metrics := index(t, md, scopeName)
	assert.Empty(t, metrics["db.client.connection.count"])
	assert.Len(t, metrics["pgbouncer.query.count"], 1)
	assert.Len(t, metrics["pgbouncer.pool.count"], 1)
}

func TestScrapeWithNoDataProducesNothing(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: map[Command][]Row{}})

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	// An empty envelope would still cost a pipeline pass downstream.
	assert.Equal(t, 0, md.DataPointCount())
	assert.Equal(t, 0, md.ResourceMetrics().Len())
}

func TestScrapeOTelOnlyEmitsNoCompatScope(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()}, promcompat.ShapeOTel)

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	assert.Empty(t, index(t, md, scopeName+promcompat.ScopeSuffix))
}

func TestScrapeCompatShapeReproducesUpstreamSeries(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()}, promcompat.ShapeOTel, promcompat.ShapePrometheus)

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	compat := index(t, md, scopeName+promcompat.ScopeSuffix)

	// A merge is reversible: each server state comes back as its own upstream
	// series, with the state carried by the name rather than by a label.
	require.Len(t, compat["pgbouncer_pools_server_active_connections"], 1)
	assert.Equal(t, int64(1), compat["pgbouncer_pools_server_active_connections"][0].IntValue())
	require.Len(t, compat["pgbouncer_pools_server_used_connections"], 1)
	assert.Equal(t, int64(2), compat["pgbouncer_pools_server_used_connections"][0].IntValue())

	// Upstream label names, not OTel attribute keys.
	labels := compat["pgbouncer_pools_server_active_connections"][0].Attributes()
	database, ok := labels.Get("database")
	require.True(t, ok)
	assert.Equal(t, "seconddb", database.AsString(), "the compat scope keeps the alias upstream reported")
	_, ok = labels.Get("db.namespace")
	assert.False(t, ok, "OTel attribute keys must not leak into the compat scope")

	// The OTel scope is untouched by compatibility mode.
	assert.Len(t, index(t, md, scopeName)["db.client.connection.count"], len(serverStates))
}

func TestScrapeCompatKeepsUnboundedLabelsNatively(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()}, promcompat.ShapeOTel, promcompat.ShapePrometheus)

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	compat := index(t, md, scopeName+promcompat.ScopeSuffix)

	// A user cutting over must see their existing series unchanged, so the
	// compat scope keeps application_name even though the OTel shape drops it.
	// It cannot be rebuilt from the OTel output, which is what makes this entry
	// native.
	dps := compat["pgbouncer_client_connections"]
	require.Len(t, dps, 3)
	_, ok := findDP(dps, "application_name", "psql")
	assert.True(t, ok)
	_, ok = findDP(dps, "application_name", "app")
	assert.True(t, ok)

	// Same for the configuration labels on the databases family.
	databases := compat["pgbouncer_databases_pool_size"]
	require.Len(t, databases, 2)
	_, ok = findDP(databases, "pool_mode", "statement")
	assert.True(t, ok)
	_, ok = findDP(databases, "force_user", "admin")
	assert.True(t, ok)
}

func TestErrorTypeIsBounded(t *testing.T) {
	// error.type must never carry a message read off the connection: it would
	// be unbounded, and could contain the target's data.
	assert.Equal(t, "timeout", errorType(context.DeadlineExceeded))
	assert.Equal(t, "canceled", errorType(context.Canceled))
	assert.Equal(t, "query_failed", errorType(errors.New("relation \"secrets\" does not exist")))
	assert.Equal(t, "", errorType(nil))
}
