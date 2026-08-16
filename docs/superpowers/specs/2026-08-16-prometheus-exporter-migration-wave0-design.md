# Wave 0 — the shared Weaver toolchain and the PgBouncer pilot

Date: 2026-08-16
Components: `telemetry/` (shared), `receiver/pgbouncerreceiver`
Issues: OTEL-329 (toolchain), OTEL-319 (pilot)

## Goal

Stand up the shared Weaver toolchain every later component in the
Prometheus-exporter migration inherits, and prove it by converting
`prometheus-community/pgbouncer_exporter` into `pgbouncerreceiver`.

The two are one piece of work. The toolchain is the deliverable; the receiver is
the evidence that the toolchain works. Neither is done until both are.

## Non-goals

- Any component other than `pgbouncerreceiver`. The toolchain's own exit
  criterion — a second component startable from registry files alone — is
  checked against `natsreceiver` in Wave 1, not here.
- A shared `grafts` base registry. Deferred until a second component exists and
  a genuinely cross-component key appears.
- Proposing anything to the semconv SIG. This spec records two candidates; both
  wait for a second proxy's worth of evidence.

## Stage 0 — triage

| Gate | Result |
| --- | --- |
| Upstream alive | `prometheus-community/pgbouncer_exporter`, 197★, last push 2026-08-09, release v0.12.1 (2026-06-26), not archived |
| Real exporter, not native `/metrics` | PgBouncer 1.25.2 exposes no HTTP endpoint; its only interface is `SHOW` over the admin console |
| No contrib receiver covers it | `contrib/receiver` has `postgresqlreceiver` only, which monitors the database, not the pooler |
| Containerizable fixture | Yes — `postgres` + `edoburu/pgbouncer` + the upstream exporter, all pinned by digest |

Passes.

## Stage 1a — the live scrape

`testdata/parity/upstream.prom` is a real capture from the fixture under load,
not a source-read. Three corrections to the issue brief came out of it, and each
one changes the work:

**46 metrics, not ~30.** 46 distinct names, 106 series with two databases and
nine concurrent client sessions.

**Seven `SHOW` commands, not six — and `SHOW SERVERS` is not among them.**
The exporter issues `SHOW LISTS`, `SHOW CONFIG`, `SHOW CLIENTS`, `SHOW STATS`,
`SHOW VERSION`, `SHOW DATABASES` and `SHOW POOLS`. It never reads `SHOW SERVERS`,
so there is no per-server metric to reach parity with. The brief's "per-server
label sets are unbounded" watch-item does not exist upstream.

**Only one metric carries an unbounded label**, and it is not the one the brief
expected. `pgbouncer_client_connections` is an *aggregate* count grouped by
`(database, user, application_name, state)` — not a per-client row. Of those,
`application_name` is client-supplied and therefore unbounded; the rest are
bounded by configuration. See the decision in Stage 2.

## Stage 1c — the access path

Pure Go via `jackc/pgx/v5` against PgBouncer's admin database. Demonstrated by a
spike that connected to the fixture and ran all eight `SHOW` commands.

**The admin console does not implement the extended query protocol.** This is
the finding that justified the spike:

```
SHOW VERSION  ERROR: extended query protocol not supported by admin console (SQLSTATE 08P01)
SHOW STATS    ERROR: failed to deallocate previously failed statement ...: FATAL: bad packet
SHOW POOLS    ERROR: failed to deallocate previously failed statement ...: conn closed
```

pgx defaults to the extended protocol. The first `SHOW` fails, the failed
statement poisons pgx's statement cache, and the deallocation attempt kills the
connection — so every subsequent command on that connection fails too. It is not
a recoverable per-query error; it takes the connection down.

The receiver must therefore set:

```go
cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
```

With it, all eight commands succeed. This is a `client.go` invariant and gets a
test that fails if the mode is ever changed.

The spike also confirmed `SHOW SERVERS` works and returns a full per-server row
set (21 columns). The upstream exporter ignores it. It is available to us as
surface beyond parity, but it is out of scope for the pilot — parity first.

## Stage 1b — build here, or extend contrib?

Build here. There is no contrib receiver for PgBouncer to extend, so the
question the playbook asks is trivially answered. Recorded for completeness.

## The mapping

### Which side of the pool is `db.client.connection.*`?

PgBouncer has two connection pools, and semconv models one. The choice of which
side takes the released convention is the single most consequential decision in
this spec.

`db.client.connection.*` describes a pool of connections held *by a client, to a
database*. PgBouncer is the client of PostgreSQL, so the **server side**
(`sv_*`, pooler→Postgres) is the semconv pool. The **client side** (`cl_*`,
app→pooler) is PgBouncer acting as a server; semconv does not model it, so it
takes a local `pgbouncer.client.connection.*` namespace.

Collapsing the two would make them indistinguishable, which is the failure mode
the brief warned about. Keeping them apart is what makes "the pool to Postgres
is saturated" and "clients are queued at the pooler" separate, answerable
questions.

### `db.client.connection.state` does not cover PgBouncer's states

The enum has exactly two members, `idle` and `used`. PgBouncer reports seven
server states. Two map cleanly and five do not:

| PgBouncer | `db.client.connection.state` | Note |
| --- | --- | --- |
| `sv_active` | `used` | Linked to a client, in use. Matches the convention. |
| `sv_idle` | `idle` | Matches. |
| `sv_used` | `used_pending_check` | **Not semconv's `used`.** |
| `sv_tested` | `tested` | Running `server_reset_query`/`server_check_query`. |
| `sv_login` | `login` | Logging in. |
| `sv_active_cancel` | `active_cancel` | Forwarding a cancel request. |
| `sv_being_canceled` | `being_canceled` | Waiting on in-flight cancels. |

The `sv_used` row is a trap worth stating plainly: PgBouncer's `sv_used` means
"idle for longer than `server_check_delay`, needs a check query before reuse" —
it is an *idle* connection. semconv's `used` means in use. Mapping the two by
name would double-count against `sv_active` and invert the meaning of the metric
people page on. It gets the local value `used_pending_check` and a `note`.

semconv enums are open, so the five local values are legal. They are recorded in
`component.yaml`'s stability block, and this asymmetry is candidate evidence for
the two-sided-pool semconv proposal the program overview leaves open.

### Alias versus real database

`SHOW POOLS` and `SHOW STATS` key on the PgBouncer *alias* — the `[databases]`
section name a client connects to. `SHOW DATABASES` is the only place the alias,
the real backend database, the host and the port appear together. In the
fixture, alias `seconddb` fronts real database `postgres` on `postgres:5432`.

`db.namespace` is "the name of the database, fully qualified within the server
address and port", which is the *real* database, and `server.address`/
`server.port` are the backend's. So the receiver joins `SHOW POOLS` and
`SHOW STATS` against `SHOW DATABASES` on the alias to resolve them. The alias
itself is kept as `pgbouncer.database.alias`.

The upstream exporter does not do this join — it emits the alias as `database`
and stops. We are deliberately more correct here, which means the compat shape
must still emit the alias under the label `database` to match. That divergence
is exactly what `compat_source` exists to record.

### `db.client.connection.pool.name`

semconv asks for a unique name and says instrumentations using a different
pattern should document it. A PgBouncer pool is identified by (alias, user), and
two pools can share an alias. The pattern is:

```
<server.address>:<server.port>/<db.namespace>/<user>
```

documented in the registry `note` and in the README.

### Conventions deliberately not used

| Convention | Why not |
| --- | --- |
| `db.client.operation.duration` | Stable and semantically exact, but a **histogram**. PgBouncer exposes only `total_query_time`, a cumulative counter. A single-bucket histogram passes review because the name looks right. Emit `pgbouncer.query.time` as a sum. |
| `db.client.connection.wait_time` | Same shape problem — histogram versus PgBouncer's `total_wait_time` counter plus a `maxwait` gauge. Emit `pgbouncer.client.wait.time` (sum) and `pgbouncer.client.wait.max` (gauge). |
| `db.client.connection.use_time` | Histogram; no PgBouncer source at all. |
| `db.client.connection.timeouts` | No PgBouncer counter exposes it. |
| `db.user` | **Deprecated** upstream — `reason: obsoleted`, "Removed, no replacement at this time" (`model/db/deprecated/registry-deprecated.yaml`). Weaver resolves a `ref` to it silently, which is why `no_deprecated_refs.rego` exists. Use local `pgbouncer.user`. |

### Full disposition table

All 46 upstream metrics are accounted for. `db.*` entries are `ref`s; everything
else is a local `pgbouncer.*` declaration.

**`SHOW POOLS` — server side → semconv**

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_pools_server_active_connections` | merge | `db.client.connection.count` `{state=used}` |
| `pgbouncer_pools_server_idle_connections` | merge | `db.client.connection.count` `{state=idle}` |
| `pgbouncer_pools_server_used_connections` | merge | `db.client.connection.count` `{state=used_pending_check}` |
| `pgbouncer_pools_server_testing_connections` | merge | `db.client.connection.count` `{state=tested}` |
| `pgbouncer_pools_server_login_connections` | merge | `db.client.connection.count` `{state=login}` |
| `pgbouncer_pools_server_active_cancel_connections` | merge | `db.client.connection.count` `{state=active_cancel}` |
| `pgbouncer_pools_server_being_canceled_connections` | merge | `db.client.connection.count` `{state=being_canceled}` |

**`SHOW POOLS` — client side → local**

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_pools_client_active_connections` | merge | `pgbouncer.client.connection.count` `{state=active}` |
| `pgbouncer_pools_client_waiting_connections` | split | `pgbouncer.client.connection.count` `{state=waiting}` **and** `db.client.connection.pending_requests` |
| `pgbouncer_pools_client_active_cancel_connections` | merge | `pgbouncer.client.connection.count` `{state=active_cancel}` |
| `pgbouncer_pools_client_waiting_cancel_connections` | merge | `pgbouncer.client.connection.count` `{state=waiting_cancel}` |
| `pgbouncer_pools_client_maxwait_seconds` | rename | `pgbouncer.client.wait.max` |

`cl_waiting` is the one deliberate `split`. It is the client-side state *and* it
is precisely semconv's "current pending requests for an open connection" against
the server pool. Emitting both means the number appears twice under different
names; the alternative is dropping the semconv-native view of pool saturation,
which is the thing users page on. **Flagged for registry review** — this is the
call most worth a second opinion.

**`SHOW STATS`** — all counters, keyed by alias

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_stats_totals_queries_pooled_total` | rename | `pgbouncer.query.count` |
| `pgbouncer_stats_totals_sql_transactions_pooled_total` | rename | `pgbouncer.transaction.count` |
| `pgbouncer_stats_totals_queries_duration_seconds_total` | rename | `pgbouncer.query.time` |
| `pgbouncer_stats_totals_server_in_transaction_seconds_total` | rename | `pgbouncer.transaction.time` |
| `pgbouncer_stats_totals_client_wait_seconds_total` | rename | `pgbouncer.client.wait.time` |
| `pgbouncer_stats_totals_server_assignments_total` | rename | `pgbouncer.server.assignment.count` |
| `pgbouncer_stats_totals_received_bytes_total` | merge | `pgbouncer.network.io` `{network.io.direction=receive}` |
| `pgbouncer_stats_totals_sent_bytes_total` | merge | `pgbouncer.network.io` `{network.io.direction=transmit}` |
| `pgbouncer_stats_totals_client_parses_total` | merge | `pgbouncer.prepared_statement.count` `{operation=parse, pgbouncer.peer=client}` |
| `pgbouncer_stats_totals_server_parses_total` | merge | `pgbouncer.prepared_statement.count` `{operation=parse, pgbouncer.peer=server}` |
| `pgbouncer_stats_totals_binds_total` | merge | `pgbouncer.prepared_statement.count` `{operation=bind}` |

`network.io.direction` is `release_candidate`, not stable — recorded in the
stability block.

`SHOW STATS`'s `avg_*` columns are dropped: they are PgBouncer-computed rates
over the same counters, and a rate belongs in the query layer.

**`SHOW DATABASES`**

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_databases_pool_size` | map | `db.client.connection.max` |
| `pgbouncer_databases_current_connections` | rename | `pgbouncer.database.connection.count` |
| `pgbouncer_databases_max_connections` | rename | `pgbouncer.database.connection.max` |
| `pgbouncer_databases_reserve_pool` | rename | `pgbouncer.database.reserve_pool.size` |
| `pgbouncer_databases_paused` | map | `pgbouncer.database.paused` |
| `pgbouncer_databases_disabled` | map | `pgbouncer.database.disabled` |
| — (`min_pool_size`, not emitted upstream) | extra | `db.client.connection.idle.min` |

**`SHOW LISTS`** — process-wide counts, all local

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_databases` | rename | `pgbouncer.database.count` |
| `pgbouncer_users` | rename | `pgbouncer.user.count` |
| `pgbouncer_pools` | rename | `pgbouncer.pool.count` |
| `pgbouncer_free_clients` | merge | `pgbouncer.client.count` `{state=free}` |
| `pgbouncer_used_clients` | merge | `pgbouncer.client.count` `{state=used}` |
| `pgbouncer_login_clients` | merge | `pgbouncer.client.count` `{state=login}` |
| `pgbouncer_free_servers` | merge | `pgbouncer.server.count` `{state=free}` |
| `pgbouncer_used_servers` | merge | `pgbouncer.server.count` `{state=used}` |
| `pgbouncer_cached_dns_names` | merge | `pgbouncer.dns.cache.count` `{type=name}` |
| `pgbouncer_cached_dns_zones` | merge | `pgbouncer.dns.cache.count` `{type=zone}` |
| `pgbouncer_in_flight_dns_queries` | rename | `pgbouncer.dns.query.count` |

**`SHOW CONFIG`**

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_config_max_client_connections` | rename | `pgbouncer.client.connection.max` |
| `pgbouncer_config_max_user_connections` | rename | `pgbouncer.user.connection.max` |

Local rather than `db.client.connection.max`, which is already the server pool's
`pool_size`. `max_client_conn` bounds the *client* side.

**`SHOW CLIENTS`** — the unbounded-label decision

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_client_connections` | map | `pgbouncer.client.connection.detail.count`, **without** `application_name` |

`application_name` is set by the connecting client. A connection-pool metric
dimensioned by an arbitrary client-supplied string is unbounded by construction,
and it is the label that turns a 3-series metric into an N-series one. It is
dropped from the OTel shape and the count aggregated over
`(db.namespace, pgbouncer.user, state)`.

It is kept in the compat shape, because a user cutting over must see their
existing series unchanged. That makes this entry `compat_source: native` — the
compat series cannot be reconstructed from OTel output, so the scraper populates
it into the compat scope directly. This is the pilot's one `native` entry and
therefore the test case for that half of `internal/promcompat`.

**`SHOW VERSION`, and the exporter's own metrics**

| Upstream | Disposition | OTel |
| --- | --- | --- |
| `pgbouncer_version_info` | resource | Resource attribute `pgbouncer.version`, and `target_info`. The compat scope carries the original series natively, because dashboards refer to it by name. |
| `pgbouncer_up` | compat-only | Scrape success describes the Collector, not PgBouncer, so it is not an OTel metric here — it is `pgbouncerreceiver.scrape.errors`. The compat scope carries it anyway, because alerting on it reaching zero is close to universal. Not an exact reproduction: the upstream exporter always answers a scrape, so an unreachable PgBouncer still yields zero, while this receiver emits nothing and the series goes stale. |
| `pgbouncer_exporter_build_info` | drop | Describes the upstream exporter binary, which we are replacing. Nothing to report. |

Counted for parity: 45 reproduced, 1 dropped with a reason.

## The toolchain

### `telemetry/weaver.sh`

Lifted from trellis: pinned Weaver, `--user "$(id -u):$(id -g)"`,
`--security-opt label=disable` so a read-only validation does not relabel the
checkout, read-only mounts, and `gofmt -w` on the output because Jinja
whitespace otherwise fails the CI diff gate.

**Resolving the upstream semconv model.** The registries depend on
`registry_path: /semconv/`, so the model tree must be present. Verified: it ships
`model/manifest.yaml` with `schema_url: https://opentelemetry.io/schemas/1.44.0`,
so it is already a Weaver registry and needs no preparation.

Decision: **fetch, do not vendor.** `weaver.sh` shallow-clones the pinned tag
into a gitignored cache and verifies the resolved commit against a SHA recorded
beside the version, so a moved tag fails rather than silently changing the
conventions. Vendoring several megabytes of YAML per version into a repository
that already pins the version in one place buys nothing and makes the semconv
bump in Stage 9 a large diff instead of a one-line one.

### Policies

Five Rego policies, each with a failing fixture:

| Policy | Fails on |
| --- | --- |
| `no_deprecated_refs` | a `ref` to an upstream key marked `deprecated` |
| `stability_disclosure` | a `development`-stability ref not listed in `component.yaml` — **not a Rego rule**, see below |
| `local_prefix` | a local declaration outside the `<system>.` prefix |
| `instrument_match` | a local metric shadowing an upstream convention it does not match on instrument type |
| `prom_annotation` | an entry whose `annotations.prometheus` lacks `compat_source`, or claims `derived` when it cannot be |

`no_deprecated_refs` is the one that closes a verified gap: `ref: db.user`
resolves clean under `weaver registry check` despite being obsoleted.

`stability_disclosure` cannot be a Rego rule. Weaver hands a policy the resolved
registry and nothing else, while this check compares it against the component's
`component.yaml`. It lives in `internal/registrycheck` instead, run by
`make telemetry-check` after the Rego pass, against `weaver.sh resolve` output.

### Templates

`telemetry/templates/registry/go/` — `weaver.yaml` plus `metrics.go.j2`
(the `pmetric` scrape-side builder), `selftelemetry.go.j2`, `promcompat.go.j2`
and `docs.md.j2`.

`metrics.go.j2` is the largest item. Unlike trellis's template, which emits OTel
API instrument constructors, it must construct `pmetric` datapoints, gate each
metric on config, attach resource attributes, and expose typed `Record…` methods.

**Fallback**, if it becomes a swamp: not mdatagen — hand-written scrapers against
a generated constants file. Still an improvement, because the constants come from
a validated registry.

### `internal/promcompat`

Implements the OTLP↔Prometheus compatibility spec — name escaping, unit and
`_total` suffixing, type mapping, resource→`target_info`, scope labelling — and
writes compat series into a separate instrumentation scope,
`<receiver scope>/promcompat`, so one `filter` processor removes them.

### Conformance harness

Docker-gated, skips cleanly without Docker. Starts PgBouncer, PostgreSQL and the
upstream exporter pinned by digest, scrapes both, and diffs on **name, type and
label-key set** into `matched` / `missing` / `extra` / `shape-mismatch`. Values
are compared for plausibility only.

Pinned digests:

```
quay.io/prometheuscommunity/pgbouncer-exporter@sha256:30f31b6c2efdad3647f8182cc7c1a3a19e42bae5d17387694989f969371c230d
edoburu/pgbouncer@sha256:7d7a27d9e90985cab5cf42256f5c13a3120baa4b055b69df37beb272b89b2340
postgres@sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73
```

The fixture mounts `pgbouncer.ini` with `security_opt: label=disable`, for the
same reason `weaver.sh` uses it: `:z` relabels the checkout on the host.

**Measured: 100%** — 45 of 45 upstream `pgbouncer_*` series reproduced with the
same name, type and label keys, no shape mismatches, one declared drop.
`parity.floor` is set to 1.0 from that measurement, replacing the program's 90%
guess. The floor carries no tolerance band: the upstream image is pinned by
digest, so the only way parity can fall is a change of ours or a deliberate pin
bump, and both deserve a failed build rather than a silent slide.

The comparison is scoped to the `pgbouncer_` namespace. An exporter mounts the
Go runtime and process collectors beside its own metrics — forty-odd series
describing the exporter binary, for which the Collector reports its own
equivalents. Counting them as missing halved the first measurement for no
reason, so `conformance.Options.Namespace` exists.

## Risks

**`metrics.go.j2` scope.** Acknowledged up front as the largest item in Wave 0
and the reason the pilot's exit criterion is the tooling rather than the
receiver. The fallback above is real and cheap to take.

**Collector API churn.** mdatagen tracks Collector releases for free; this
template does not. Mitigated by a narrow surface — pdata construction is stable
and `grafts` pins Collector versions repo-wide — but it is a standing cost.

**`development`-stability conventions.** Every `db.client.connection.*` metric
is `development`, and `network.io.direction` is `release_candidate`. Only
`db.namespace`, `db.system.name`, `server.address` and `server.port` are stable.
The README must not present the rest as settled, and a semconv bump triggers the
Stage 9 re-review.

**`definition/2` is not yet stable** in Weaver v0.25.1. trellis already accepts
this bet; we inherit it.

## Decisions taken during implementation

**Generated `Record` methods take an attributes struct, not positional
parameters.** Several metrics here carry five or more same-typed strings, and a
transposed pair compiles cleanly and produces mislabelled datapoints. The
scraper tests caught three such bugs before the change; named fields make them
compile errors. Every later component inherits this.

**Identifiers keep every segment of the metric name.** trellis's templates drop
the root because one registry owns one prefix. A grafts registry is mostly refs
to upstream, so dropping it collides `db.client.connection.count` with
`pgbouncer.client.connection.count`.

**The registry carries a datapoint-precise compat mapping.** Naming the upstream
series was not enough to generate `promcompat` from: an entry now carries
`labels`, mapping an attribute key to its Prometheus label, and `series` with a
`when` that selects datapoints by attribute value. That is what makes a merge
reversible rather than merely documented, and a policy rule checks those keys
against the metric's own attributes.

**`scrapererror` is not used.** It exists for `scraperhelper`, which this
receiver does not use any more than `snmpreceiver` does. Partial scrapes are
joined errors, counted in self-telemetry.

## Open questions for review

1. The `cl_waiting` split — emitting the same number as both
   `pgbouncer.client.connection.count{state=waiting}` and
   `db.client.connection.pending_requests`. Deliberate, and the call most worth
   a second opinion.
2. Whether `pgbouncer.client.connection.detail.count` (the `SHOW CLIENTS`
   aggregate, minus `application_name`) earns its place at all, given it largely
   restates `pgbouncer.client.connection.count` from `SHOW POOLS` at a different
   grain.
3. Whether to expose `SHOW SERVERS` in a later iteration. It is real surface the
   upstream exporter lacks, but every row is per-connection and the aggregation
   decision is not free. Not in the pilot.
