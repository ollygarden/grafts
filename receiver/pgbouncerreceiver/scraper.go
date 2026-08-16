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
	cfg    *Config
	client client
	mb     *telemetry.MetricsBuilder
	self   *telemetry.SelfTelemetry
	compat *promcompat.Table

	// Resolved once: the endpoint does not change, and re-parsing it every
	// scrape to build a resource that is then copied wholesale is pure waste.
	host string
	port int64

	version string
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

	if s.cfg.Emit.Has(promcompat.ShapePrometheus) && md.ResourceMetrics().Len() > 0 {
		s.compat.Append(md)
		errs = append(errs, s.recordNativeCompat(md, clients, dbRows, len(errs) == 0)...)
	}

	s.self.RecordScrapeDuration(ctx, time.Since(started).Seconds())
	return md, errors.Join(errs...)
}

// resource describes the monitored PgBouncer instance.
func (s *scraper) resource() pcommon.Resource {
	res := pcommon.NewResource()
	res.Attributes().PutStr("db.system.name", "postgresql")
	res.Attributes().PutStr("server.address", s.host)
	res.Attributes().PutInt("server.port", s.port)
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

		pool := poolName(db, "")
		s.mb.RecordDBClientConnectionMax(now, row.Int("pool_size"), telemetry.DBClientConnectionMaxAttributes{
			DBClientConnectionPoolName: pool,
			DBNamespace:                db.namespace,
			PgbouncerDatabaseAlias:     db.alias,
			ServerAddress:              db.host,
			ServerPort:                 db.port,
		})
		s.mb.RecordDBClientConnectionIdleMin(now, row.Int("min_pool_size"), telemetry.DBClientConnectionIdleMinAttributes{
			DBClientConnectionPoolName: pool,
			DBNamespace:                db.namespace,
			PgbouncerDatabaseAlias:     db.alias,
			ServerAddress:              db.host,
			ServerPort:                 db.port,
		})
		s.mb.RecordPgbouncerDatabaseConnectionCount(now, row.Int("current_connections"), telemetry.PgbouncerDatabaseConnectionCountAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, ServerAddress: db.host, ServerPort: db.port,
		})
		s.mb.RecordPgbouncerDatabaseConnectionMax(now, row.Int("max_connections"), telemetry.PgbouncerDatabaseConnectionMaxAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, ServerAddress: db.host, ServerPort: db.port,
		})
		s.mb.RecordPgbouncerDatabaseReservePoolSize(now, row.Int("reserve_pool_size"), telemetry.PgbouncerDatabaseReservePoolSizeAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, ServerAddress: db.host, ServerPort: db.port,
		})
		s.mb.RecordPgbouncerDatabasePaused(now, row.Int("paused"), telemetry.PgbouncerDatabasePausedAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, ServerAddress: db.host, ServerPort: db.port,
		})
		s.mb.RecordPgbouncerDatabaseDisabled(now, row.Int("disabled"), telemetry.PgbouncerDatabaseDisabledAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, ServerAddress: db.host, ServerPort: db.port,
		})
	}
	return index
}

// recordPools reports both sides of every pool.
func (s *scraper) recordPools(now pcommon.Timestamp, rows []Row, databases map[string]database) {
	for _, row := range rows {
		alias, user := row["database"], row["user"]
		db := lookup(databases, alias)
		pool := poolName(db, user)

		for _, st := range serverStates {
			s.mb.RecordDBClientConnectionCount(now, row.Int(st.column), telemetry.DBClientConnectionCountAttributes{
				DBClientConnectionPoolName: pool,
				DBClientConnectionState:    st.state,
				DBNamespace:                db.namespace,
				PgbouncerDatabaseAlias:     db.alias,
				PgbouncerUser:              user,
				ServerAddress:              db.host,
				ServerPort:                 db.port,
			})
		}
		for _, st := range clientStates {
			s.mb.RecordPgbouncerClientConnectionCount(now, row.Int(st.column), telemetry.PgbouncerClientConnectionCountAttributes{
				DBNamespace:                    db.namespace,
				PgbouncerClientConnectionState: st.state,
				PgbouncerDatabaseAlias:         db.alias,
				PgbouncerUser:                  user,
				ServerAddress:                  db.host,
				ServerPort:                     db.port,
			})
		}

		// cl_waiting is reported twice on purpose: as a client-side state
		// above, and here as the convention's view of server-pool saturation.
		s.mb.RecordDBClientConnectionPendingRequests(now, row.Int("cl_waiting"), telemetry.DBClientConnectionPendingRequestsAttributes{
			DBClientConnectionPoolName: pool,
			DBNamespace:                db.namespace,
			PgbouncerDatabaseAlias:     db.alias,
			PgbouncerUser:              user,
			ServerAddress:              db.host,
			ServerPort:                 db.port,
		})

		// maxwait_us is the sub-second remainder of maxwait, not a separate
		// measurement -- adding them is what makes this a real duration.
		maxwait := row.Float("maxwait") + row.Float("maxwait_us")/1e6
		s.mb.RecordPgbouncerClientWaitMax(now, maxwait, telemetry.PgbouncerClientWaitMaxAttributes{
			DBNamespace: db.namespace, PgbouncerDatabaseAlias: db.alias, PgbouncerUser: user,
		})
	}
}

// recordStats reports the traffic pooled per database.
func (s *scraper) recordStats(now pcommon.Timestamp, rows []Row, databases map[string]database) {
	for _, row := range rows {
		db := lookup(databases, row["database"])
		ns := db.namespace

		s.mb.RecordPgbouncerQueryCount(now, row.Int("total_query_count"),
			telemetry.PgbouncerQueryCountAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})
		s.mb.RecordPgbouncerTransactionCount(now, row.Int("total_xact_count"),
			telemetry.PgbouncerTransactionCountAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})
		s.mb.RecordPgbouncerServerAssignmentCount(now, row.Int("total_server_assignment_count"),
			telemetry.PgbouncerServerAssignmentCountAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})

		// PgBouncer reports these in microseconds; the conventions are seconds.
		s.mb.RecordPgbouncerQueryTime(now, row.Float("total_query_time")/1e6,
			telemetry.PgbouncerQueryTimeAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})
		s.mb.RecordPgbouncerTransactionTime(now, row.Float("total_xact_time")/1e6,
			telemetry.PgbouncerTransactionTimeAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})
		s.mb.RecordPgbouncerClientWaitTime(now, row.Float("total_wait_time")/1e6,
			telemetry.PgbouncerClientWaitTimeAttributes{DBNamespace: ns, PgbouncerDatabaseAlias: db.alias})

		s.mb.RecordPgbouncerNetworkIO(now, row.Int("total_received"), telemetry.PgbouncerNetworkIOAttributes{
			DBNamespace: ns, NetworkIODirection: telemetry.AttributeNetworkIODirectionReceive, PgbouncerDatabaseAlias: db.alias,
		})
		s.mb.RecordPgbouncerNetworkIO(now, row.Int("total_sent"), telemetry.PgbouncerNetworkIOAttributes{
			DBNamespace: ns, NetworkIODirection: telemetry.AttributeNetworkIODirectionTransmit, PgbouncerDatabaseAlias: db.alias,
		})

		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_client_parse_count"), telemetry.PgbouncerPreparedStatementCountAttributes{
			DBNamespace: ns, PgbouncerDatabaseAlias: db.alias,
			PgbouncerPeer:                       telemetry.AttributePgbouncerPeerClient,
			PgbouncerPreparedStatementOperation: telemetry.AttributePgbouncerPreparedStatementOperationParse,
		})
		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_server_parse_count"), telemetry.PgbouncerPreparedStatementCountAttributes{
			DBNamespace: ns, PgbouncerDatabaseAlias: db.alias,
			PgbouncerPeer:                       telemetry.AttributePgbouncerPeerServer,
			PgbouncerPreparedStatementOperation: telemetry.AttributePgbouncerPreparedStatementOperationParse,
		})
		// A Bind message has no peer: it is counted once, not on both sides.
		s.mb.RecordPgbouncerPreparedStatementCount(now, row.Int("total_bind_count"), telemetry.PgbouncerPreparedStatementCountAttributes{
			DBNamespace: ns, PgbouncerDatabaseAlias: db.alias,
			PgbouncerPreparedStatementOperation: telemetry.AttributePgbouncerPreparedStatementOperationBind,
		})
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

	s.mb.RecordPgbouncerClientCount(now, lists["free_clients"], telemetry.PgbouncerClientCountAttributes{PgbouncerClientState: telemetry.AttributePgbouncerClientStateFree})
	s.mb.RecordPgbouncerClientCount(now, lists["used_clients"], telemetry.PgbouncerClientCountAttributes{PgbouncerClientState: telemetry.AttributePgbouncerClientStateUsed})
	s.mb.RecordPgbouncerClientCount(now, lists["login_clients"], telemetry.PgbouncerClientCountAttributes{PgbouncerClientState: telemetry.AttributePgbouncerClientStateLogin})

	s.mb.RecordPgbouncerServerCount(now, lists["free_servers"], telemetry.PgbouncerServerCountAttributes{PgbouncerServerState: telemetry.AttributePgbouncerServerStateFree})
	s.mb.RecordPgbouncerServerCount(now, lists["used_servers"], telemetry.PgbouncerServerCountAttributes{PgbouncerServerState: telemetry.AttributePgbouncerServerStateUsed})

	s.mb.RecordPgbouncerDNSCacheCount(now, lists["dns_names"], telemetry.PgbouncerDNSCacheCountAttributes{PgbouncerDNSCacheType: telemetry.AttributePgbouncerDNSCacheTypeName})
	s.mb.RecordPgbouncerDNSCacheCount(now, lists["dns_zones"], telemetry.PgbouncerDNSCacheCountAttributes{PgbouncerDNSCacheType: telemetry.AttributePgbouncerDNSCacheTypeZone})
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
	counts := make(map[key]int64, len(rows))
	for _, row := range rows {
		counts[key{row["database"], row["user"], row["state"]}]++
	}
	for k, count := range counts {
		db := lookup(databases, k.alias)
		s.mb.RecordPgbouncerClientConnectionDetailCount(now, count, telemetry.PgbouncerClientConnectionDetailCountAttributes{
			DBNamespace:            db.namespace,
			PgbouncerClientState:   k.state,
			PgbouncerDatabaseAlias: db.alias,
			PgbouncerUser:          k.user,
		})
	}
}

// recordNativeCompat writes the compat series that cannot be rebuilt from the
// OTel output, because the OTel shape drops labels they carry.
func (s *scraper) recordNativeCompat(md pmetric.Metrics, clients, databases []Row, complete bool) []error {
	dst := s.compat.Scope(md.ResourceMetrics().At(0))
	var errs []error
	native := func(name string, value float64, labels map[string]string) {
		if err := s.compat.AppendNative(dst, name, value, true, labels); err != nil {
			errs = append(errs, err)
		}
	}

	// Neither of the next two is in the registry, and neither can be: the
	// registry declares OTel metrics and their compat views, while these
	// describe the scrape and the process rather than a measurement. Both are
	// listed in component.yaml's compat_only with a reason.
	//
	// Alerting on `pgbouncer_up` reaching zero is close to universal, so the
	// compat scope carries it rather than leaving those alerts silently broken.
	// It is not an exact reproduction: the upstream exporter always answers a
	// scrape, so an unreachable PgBouncer still yields zero, while this receiver
	// emits nothing at all and the series goes stale instead. Alert on absence
	// as well as on zero.
	up := int64(0)
	if complete {
		up = 1
	}
	promcompat.AppendUndeclared(dst, "pgbouncer_up", "gauge", up, nil)

	// The version is a resource attribute in the OTel shape and reaches
	// Prometheus through target_info, but dashboards refer to this name.
	if s.version != "" {
		promcompat.AppendUndeclared(dst, "pgbouncer_version_info", "gauge", 1, map[string]string{"version": s.version})
	}

	// pgbouncer_client_connections keeps application_name, which the OTel shape
	// drops as unbounded.
	type clientKey struct{ database, user, appName, state string }
	counts := make(map[clientKey]int64, len(clients))
	for _, row := range clients {
		counts[clientKey{row["database"], row["user"], row["application_name"], row["state"]}]++
	}
	for k, count := range counts {
		native("pgbouncer_client_connections", float64(count), map[string]string{
			"database":         k.database,
			"user":             k.user,
			"application_name": k.appName,
			"state":            k.state,
		})
	}

	// The pgbouncer_databases_* family keeps force_user and pool_mode, which are
	// configuration rather than measurement and so stay out of the OTel shape.
	for _, row := range databases {
		labels := map[string]string{
			"name":       row["name"],
			"database":   row["database"],
			"host":       row["host"],
			"port":       row["port"],
			"force_user": row["force_user"],
			"pool_mode":  row["pool_mode"],
		}
		for _, m := range databaseCompat {
			native(m.series, float64(row.Int(m.column)), labels)
		}
	}
	return errs
}

// databaseCompat maps each SHOW DATABASES column to the upstream series that
// carries it. The names are checked against the registry by AppendNative, so a
// rename stays a registry change rather than a search for string literals.
var databaseCompat = []struct{ series, column string }{
	{"pgbouncer_databases_pool_size", "pool_size"},
	{"pgbouncer_databases_current_connections", "current_connections"},
	{"pgbouncer_databases_max_connections", "max_connections"},
	{"pgbouncer_databases_reserve_pool", "reserve_pool_size"},
	{"pgbouncer_databases_paused", "paused"},
	{"pgbouncer_databases_disabled", "disabled"},
}

// lookup resolves an alias to its database, falling back to a stand-in when
// SHOW DATABASES did not report it. Keeping the alias means the datapoint stays
// attributable rather than being dropped.
func lookup(databases map[string]database, alias string) database {
	if db, ok := databases[alias]; ok {
		return db
	}
	return database{alias: alias, namespace: alias}
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
