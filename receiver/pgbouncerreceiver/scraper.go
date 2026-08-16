package pgbouncerreceiver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"go.olly.garden/grafts/internal/promcompat"
	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// PgBouncer reports seven server-connection states and
// `db.client.connection.state` defines two. Semantic-convention enums are open,
// so the remaining five carry these values.
//
// StateUsedPendingCheck is the one that must not be spelled `used`: PgBouncer's
// `sv_used` means idle for longer than `server_check_delay` and needing a check
// query before reuse. Reporting it as the convention's `used` -- which means in
// use -- would double-count against `sv_active` and invert the metric people
// alert on.
const (
	StateUsedPendingCheck = "used_pending_check"
	StateTested           = "tested"
	StateLogin            = "login"
	StateActiveCancel     = "active_cancel"
	StateBeingCanceled    = "being_canceled"
)

// serverStates maps SHOW POOLS' server columns onto connection states.
var serverStates = []struct{ column, state string }{
	{"sv_active", telemetry.AttributeDBClientConnectionStateUsed},
	{"sv_idle", telemetry.AttributeDBClientConnectionStateIdle},
	{"sv_used", StateUsedPendingCheck},
	{"sv_tested", StateTested},
	{"sv_login", StateLogin},
	{"sv_active_cancel", StateActiveCancel},
	{"sv_being_canceled", StateBeingCanceled},
}

// clientStates maps SHOW POOLS' client columns onto client states.
var clientStates = []struct{ column, state string }{
	{"cl_active", telemetry.AttributePgbouncerClientConnectionStateActive},
	{"cl_waiting", telemetry.AttributePgbouncerClientConnectionStateWaiting},
	{"cl_active_cancel_req", telemetry.AttributePgbouncerClientConnectionStateActiveCancel},
	{"cl_waiting_cancel_req", telemetry.AttributePgbouncerClientConnectionStateWaitingCancel},
}

// database is one row of SHOW DATABASES.
//
// SHOW POOLS and SHOW STATS report only PgBouncer's alias for a database. This
// is the only place the alias, the backend database, and the backend's host and
// port appear together, so it is what lets the receiver set `db.namespace`,
// `server.address` and `server.port` to the backend rather than to the alias.
// The upstream exporter does not make that join.
type database struct {
	alias     string
	namespace string
	host      string
	port      int64
	forceUser string
	poolMode  string
}

// scraper turns one PgBouncer admin scrape into metrics.
type scraper struct {
	cfg          *Config
	client       client
	mb           *telemetry.MetricsBuilder
	self         *telemetry.SelfTelemetry
	scopeName    string
	scopeVersion string
	version      string
}

// scrape collects one round of metrics.
//
// It is deliberately partial: a command that fails is recorded and skipped, and
// everything else is still reported. That matches the upstream exporter, whose
// users rely on the remaining metrics surviving a single failing command.
func (s *scraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	started := time.Now()
	now := pcommon.NewTimestampFromTime(started)

	var errs []error
	query := func(cmd Command) []Row {
		rows, err := s.client.Query(ctx, cmd)
		if err != nil {
			errs = append(errs, err)
			s.self.RecordScrapeErrors(ctx, 1, errorType(err), string(cmd))
			return nil
		}
		return rows
	}

	dbRows := query(CommandDatabases)
	databases := s.recordDatabases(now, dbRows)
	s.recordPools(now, query(CommandPools), databases)
	s.recordStats(now, query(CommandStats), databases)
	s.recordLists(now, query(CommandLists))
	s.recordConfig(now, query(CommandConfig))
	clients := query(CommandClients)
	s.recordClients(now, clients, databases)

	if rows := query(CommandVersion); len(rows) > 0 {
		s.version = rows[0]["version"]
	}

	md := s.mb.Emit(s.resource())

	if s.cfg.emits(ShapePrometheus) && md.ResourceMetrics().Len() > 0 {
		if err := promcompat.Append(md, telemetry.CompatTable, s.scopeName, s.scopeVersion); err != nil {
			errs = append(errs, err)
		} else {
			s.recordNativeCompat(md, clients, dbRows)
		}
	}

	s.self.RecordScrapeDuration(ctx, time.Since(started).Seconds())
	return md, errors.Join(errs...)
}

// resource describes the monitored PgBouncer instance.
func (s *scraper) resource() pcommon.Resource {
	res := pcommon.NewResource()
	res.Attributes().PutStr("db.system.name", "postgresql")
	res.Attributes().PutStr("server.address", host(s.cfg.Endpoint))
	res.Attributes().PutInt("server.port", port(s.cfg.Endpoint))
	if s.version != "" {
		res.Attributes().PutStr("pgbouncer.version", s.version)
	}
	return res
}

// recordDatabases reports the configured databases and returns them indexed by
// alias, for the pool and stats joins.
func (s *scraper) recordDatabases(now pcommon.Timestamp, rows []Row) map[string]database {
	index := make(map[string]database, len(rows))
	for _, row := range rows {
		db := database{
			alias:     row["name"],
			namespace: row["database"],
			host:      row["host"],
			port:      row.Int("port"),
			forceUser: row["force_user"],
			poolMode:  row["pool_mode"],
		}
		index[db.alias] = db

		s.mb.RecordDBClientConnectionMax(now, row.Int("pool_size"),
			poolName(db, ""), db.namespace, db.alias, db.host, db.port)
		s.mb.RecordDBClientConnectionIdleMin(now, row.Int("min_pool_size"),
			poolName(db, ""), db.namespace, db.alias, db.host, db.port)
		s.mb.RecordPgbouncerDatabaseConnectionCount(now, row.Int("current_connections"),
			db.namespace, db.alias, db.host, db.port)
		s.mb.RecordPgbouncerDatabaseConnectionMax(now, row.Int("max_connections"),
			db.namespace, db.alias, db.host, db.port)
		s.mb.RecordPgbouncerDatabaseReservePoolSize(now, row.Int("reserve_pool_size"),
			db.namespace, db.alias, db.host, db.port)
		s.mb.RecordPgbouncerDatabasePaused(now, row.Int("paused"),
			db.namespace, db.alias, db.host, db.port)
		s.mb.RecordPgbouncerDatabaseDisabled(now, row.Int("disabled"),
			db.namespace, db.alias, db.host, db.port)
	}
	return index
}

// recordPools reports both sides of every pool.
func (s *scraper) recordPools(now pcommon.Timestamp, rows []Row, databases map[string]database) {
	for _, row := range rows {
		alias, user := row["database"], row["user"]
		db := databases[alias]
		if db.alias == "" {
			// A pool for a database SHOW DATABASES did not report. Keep the
			// alias so the datapoint is still attributable, rather than
			// dropping the pool.
			db = database{alias: alias, namespace: alias}
		}
		pool := poolName(db, user)

		for _, st := range serverStates {
			s.mb.RecordDBClientConnectionCount(now, row.Int(st.column),
				pool, st.state, db.namespace, db.alias, user, db.host, db.port)
		}
		for _, st := range clientStates {
			s.mb.RecordPgbouncerClientConnectionCount(now, row.Int(st.column),
				st.state, db.namespace, db.alias, user, db.host, db.port)
		}

		// cl_waiting is reported twice on purpose: as a client-side state
		// above, and here as the convention's view of server-pool saturation.
		s.mb.RecordDBClientConnectionPendingRequests(now, row.Int("cl_waiting"),
			pool, db.namespace, db.alias, user, db.host, db.port)

		// maxwait_us is the sub-second remainder of maxwait, not a separate
		// measurement -- adding them is what makes this a real duration.
		maxwait := row.Float("maxwait") + row.Float("maxwait_us")/1e6
		s.mb.RecordPgbouncerClientWaitMax(now, maxwait, db.namespace, db.alias, user)
	}
}

// recordStats reports the traffic pooled per database.
func (s *scraper) recordStats(now pcommon.Timestamp, rows []Row, databases map[string]database) {
	for _, row := range rows {
		alias := row["database"]
		db := databases[alias]
		if db.alias == "" {
			db = database{alias: alias, namespace: alias}
		}
		ns := db.namespace

		s.mb.RecordPgbouncerQueryCount(now, row.Int("total_query_count"), ns, db.alias)
		s.mb.RecordPgbouncerTransactionCount(now, row.Int("total_xact_count"), ns, db.alias)
		s.mb.RecordPgbouncerServerAssignmentCount(now, row.Int("total_server_assignment_count"), ns, db.alias)

		// PgBouncer reports these in microseconds; the conventions are seconds.
		s.mb.RecordPgbouncerQueryTime(now, row.Float("total_query_time")/1e6, ns, db.alias)
		s.mb.RecordPgbouncerTransactionTime(now, row.Float("total_xact_time")/1e6, ns, db.alias)
		s.mb.RecordPgbouncerClientWaitTime(now, row.Float("total_wait_time")/1e6, ns, db.alias)

		s.mb.RecordPgbouncerNetworkIO(now, row.Int("total_received"),
			ns, telemetry.AttributeNetworkIODirectionReceive, db.alias)
		s.mb.RecordPgbouncerNetworkIO(now, row.Int("total_sent"),
			ns, telemetry.AttributeNetworkIODirectionTransmit, db.alias)

		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_client_parse_count"),
			ns, telemetry.AttributePgbouncerPeerClient, db.alias,
			telemetry.AttributePgbouncerPreparedStatementOperationParse)
		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_server_parse_count"),
			ns, telemetry.AttributePgbouncerPeerServer, db.alias,
			telemetry.AttributePgbouncerPreparedStatementOperationParse)
		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_bind_count"),
			ns, "", db.alias,
			telemetry.AttributePgbouncerPreparedStatementOperationBind)
	}
}

// recordLists reports the process-wide counts SHOW LISTS returns as one
// name/value row per item.
func (s *scraper) recordLists(now pcommon.Timestamp, rows []Row) {
	if len(rows) == 0 {
		return
	}
	lists := make(map[string]int64, len(rows))
	for _, row := range rows {
		lists[row["list"]] = row.Int("items")
	}

	s.mb.RecordPgbouncerDatabaseCount(now, lists["databases"])
	s.mb.RecordPgbouncerUserCount(now, lists["users"])
	s.mb.RecordPgbouncerPoolCount(now, lists["pools"])
	s.mb.RecordPgbouncerDNSQueryCount(now, lists["dns_pending"])

	s.mb.RecordPgbouncerClientCount(now, lists["free_clients"], telemetry.AttributePgbouncerClientStateFree)
	s.mb.RecordPgbouncerClientCount(now, lists["used_clients"], telemetry.AttributePgbouncerClientStateUsed)
	s.mb.RecordPgbouncerClientCount(now, lists["login_clients"], telemetry.AttributePgbouncerClientStateLogin)

	s.mb.RecordPgbouncerServerCount(now, lists["free_servers"], telemetry.AttributePgbouncerServerStateFree)
	s.mb.RecordPgbouncerServerCount(now, lists["used_servers"], telemetry.AttributePgbouncerServerStateUsed)

	s.mb.RecordPgbouncerDNSCacheCount(now, lists["dns_names"], telemetry.AttributePgbouncerDNSCacheTypeName)
	s.mb.RecordPgbouncerDNSCacheCount(now, lists["dns_zones"], telemetry.AttributePgbouncerDNSCacheTypeZone)
}

// recordConfig reports the two limits the upstream exporter surfaces from
// SHOW CONFIG, which returns one key/value row per setting.
func (s *scraper) recordConfig(now pcommon.Timestamp, rows []Row) {
	for _, row := range rows {
		value, err := strconv.ParseInt(row["value"], 10, 64)
		if err != nil {
			continue
		}
		switch row["key"] {
		case "max_client_conn":
			s.mb.RecordPgbouncerClientConnectionMax(now, value)
		case "max_user_connections":
			s.mb.RecordPgbouncerUserConnectionMax(now, value)
		}
	}
}

// recordClients reports SHOW CLIENTS aggregated over (database, user, state).
//
// application_name is deliberately not a dimension: it is set by the connecting
// client, so a metric carrying it is unbounded by construction. The compat
// scope keeps it, which is why this entry is the component's one native one.
func (s *scraper) recordClients(now pcommon.Timestamp, rows []Row, databases map[string]database) {
	type key struct{ alias, user, state string }
	counts := make(map[key]int64)
	for _, row := range rows {
		counts[key{row["database"], row["user"], row["state"]}]++
	}
	for k, count := range counts {
		db := databases[k.alias]
		if db.alias == "" {
			db = database{alias: k.alias, namespace: k.alias}
		}
		s.mb.RecordPgbouncerClientConnectionDetailCount(now, count,
			db.namespace, db.alias, k.state, k.user)
	}
}

// recordNativeCompat writes the compat series that cannot be rebuilt from the
// OTel output, because the OTel shape drops labels they carry.
func (s *scraper) recordNativeCompat(md pmetric.Metrics, clients, databases []Row) {
	if md.ResourceMetrics().Len() == 0 {
		return
	}
	dst := promcompat.Scope(md.ResourceMetrics().At(0), s.scopeName, s.scopeVersion)

	// pgbouncer_client_connections keeps application_name.
	type clientKey struct{ database, user, appName, state string }
	counts := make(map[clientKey]int64)
	for _, row := range clients {
		counts[clientKey{row["database"], row["user"], row["application_name"], row["state"]}]++
	}
	for k, count := range counts {
		promcompat.AppendNative(dst, "pgbouncer_client_connections", "gauge", count, map[string]string{
			"database":         k.database,
			"user":             k.user,
			"application_name": k.appName,
			"state":            k.state,
		})
	}

	// The pgbouncer_databases_* family keeps force_user and pool_mode, which
	// are configuration rather than measurement and so stay out of the OTel
	// shape.
	for _, row := range databases {
		labels := map[string]string{
			"name":       row["name"],
			"database":   row["database"],
			"host":       row["host"],
			"port":       row["port"],
			"force_user": row["force_user"],
			"pool_mode":  row["pool_mode"],
		}
		for name, column := range map[string]string{
			"pgbouncer_databases_pool_size":           "pool_size",
			"pgbouncer_databases_current_connections": "current_connections",
			"pgbouncer_databases_max_connections":     "max_connections",
			"pgbouncer_databases_reserve_pool":        "reserve_pool_size",
			"pgbouncer_databases_paused":              "paused",
			"pgbouncer_databases_disabled":            "disabled",
		} {
			promcompat.AppendNative(dst, name, "gauge", row.Int(column), labels)
		}
	}
}

// poolName builds `db.client.connection.pool.name`.
//
// The convention asks for a name unique within the instrumented application and
// says an instrumentation using a different pattern should document it. A
// PgBouncer pool is identified by (alias, user) and two pools can share an
// alias, so the user is part of the name; the backend address distinguishes
// aliases that route elsewhere.
func poolName(db database, user string) string {
	name := fmt.Sprintf("%s:%d/%s", db.host, db.port, db.namespace)
	if user != "" {
		name += "/" + user
	}
	return name
}

// errorType classifies a scrape failure for `error.type`. It never carries a
// message read off the connection, which would be unbounded and could contain
// the target's data.
func errorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "query_failed"
	}
}
