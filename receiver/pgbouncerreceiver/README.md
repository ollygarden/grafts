# PgBouncer receiver

Polls [PgBouncer](https://www.pgbouncer.org/)'s admin console for connection-pool
metrics, as a replacement for
[`prometheus-community/pgbouncer_exporter`](https://github.com/prometheus-community/pgbouncer_exporter).

PgBouncer sits in front of most serious PostgreSQL deployments, and
`postgresqlreceiver` monitors the database rather than the pooler. Pool
saturation and client wait are exactly the symptoms people get paged for, and
the Collector could not see them.

Measured parity with `pgbouncer_exporter` v0.12.1 is **100%** — 45 of 45
`pgbouncer_*` series, with no shape mismatches. That number is computed by an
integration test, not claimed; see [parity-report.md](parity-report.md).

## Configuration

```yaml
receivers:
  pgbouncer:
    endpoint: localhost:6432
    username: pgbouncer
    password: ${env:PGBOUNCER_PASSWORD}
    database: pgbouncer
    tls: disable
    collection_interval: 60s
    timeout: 10s
    emit: [otel]
```

| Setting | Default | Description |
| --- | --- | --- |
| `endpoint` | `localhost:6432` | PgBouncer's listen address, as `host:port`. |
| `username` | — | Required. A role in PgBouncer's `stats_users` or `admin_users`. |
| `password` | — | Password for `username`. |
| `database` | `pgbouncer` | PgBouncer's admin database. |
| `tls` | `disable` | libpq `sslmode`. The admin console is usually reached over loopback or a private network. |
| `collection_interval` | `60s` | How often PgBouncer is polled. |
| `timeout` | `10s` | Bounds one scrape across all commands. Must be shorter than `collection_interval`. |
| `emit` | `[otel]` | Output shapes: `otel`, `prometheus`, or both. |
| `metrics` | all enabled | Per-metric `enabled` flags, keyed by metric name. |

The role must appear in `stats_users` or `admin_users`. A role in neither
connects successfully and then sees nothing, which looks like a broken receiver
rather than a permissions problem.

### Required PgBouncer configuration

```ini
[pgbouncer]
stats_users = pgbouncer
```

## Compatibility mode

`emit: [otel, prometheus]` additionally emits the series `pgbouncer_exporter`
produced, in a **separate instrumentation scope** named
`go.olly.garden/grafts/receiver/pgbouncerreceiver/promcompat`. That separability
is the point: existing dashboards and alerts keep working while you migrate, and
one processor removes the compat set once you are done.

```yaml
processors:
  filter:
    metrics:
      exclude:
        match_type: expr
        expressions:
          - instrumentation_scope.name endsWith "/promcompat"
```

Compatibility mode roughly doubles the series count, which is why it is off by
default.

**One difference worth knowing about.** `pgbouncer_up` is emitted, because
alerting on it reaching zero is close to universal. It is not an exact
reproduction: the upstream exporter always answers a scrape, so an unreachable
PgBouncer still yields `pgbouncer_up 0`, while this receiver emits nothing at all
and the series goes stale instead. Alert on absence as well as on zero.

## What it reports

The server side of the pooler — PgBouncer's connections *to* PostgreSQL — uses
the released `db.client.connection.*` conventions, because PgBouncer is
PostgreSQL's client there. The client side — applications' connections *to*
PgBouncer — has no released convention and uses `pgbouncer.client.connection.*`.

Keeping them apart is deliberate. Collapsing them would make "the pool to
Postgres is saturated" and "clients are queued at the pooler" the same number,
and those have different fixes.

See [migration.md](migration.md) for the generated table mapping every upstream
series to what it becomes here.

### Two mappings worth reading before you write an alert

**`db.client.connection.state`** defines `idle` and `used`; PgBouncer reports
seven server states. `sv_active` is reported as `used` and `sv_idle` as `idle`.
The other five carry local values, and one of them is a trap: PgBouncer's
`sv_used` means *idle for longer than `server_check_delay`, needing a check
query* — an idle connection — so it is reported as `used_pending_check`, **not**
as `used`. Treating it as `used` would double-count against `sv_active` and
invert the metric.

**`db.namespace` is the backend database, not the alias.** `SHOW POOLS` and
`SHOW STATS` report only PgBouncer's `[databases]` alias; the receiver joins them
against `SHOW DATABASES` so `db.namespace`, `server.address` and `server.port`
describe the real backend. The alias is kept as `pgbouncer.database.alias`. The
upstream exporter does not make that join, so the compat scope still reports the
alias under the label `database`.

### Stability

Most of what this receiver reports is `development`-stability convention and can
still be renamed by upstream OpenTelemetry. Only `db.namespace`,
`db.system.name`, `server.address` and `server.port` are stable;
`network.io.direction` is release-candidate. The full list is in
[telemetry/component.yaml](telemetry/component.yaml), and it is enforced — a
convention this receiver leans on that is not listed there fails the build.

### Cardinality

`application_name` is deliberately not a dimension in the OTel shape. It is set
by the connecting client, so a metric carrying it is unbounded by construction.
The compat scope keeps it, because a user cutting over must see their existing
series unchanged.

## Development

The telemetry is defined in [telemetry/registry/](telemetry/registry/) and
generated; do not hand-edit `internal/telemetry/`.

```bash
make telemetry-check      # validate the registry and prove the policies fire
make telemetry-generate   # regenerate internal/telemetry and migration.md
go test ./receiver/pgbouncerreceiver/...
make test-integration     # conformance against the real exporter; needs Docker
```
