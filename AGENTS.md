# AGENTS.md

Guidance for coding agents working in this repository. **Read
[CONTRIBUTING.md](CONTRIBUTING.md) first** for project-wide conventions
(building, testing, commits, instrumentation). This file is the codebase map and
the development workflow.

## Overview

Grafts is a collection of custom OpenTelemetry Collector components for
OllyGarden. Components are "grafted" onto the standard collector via the
OpenTelemetry Collector Builder (OCB).

## Development workflow

Applies to **feature or behavior changes** (new components, features, behavior
changes). Trivial fixes (typos, comments, tiny localized bugfixes) skip
brainstorm/spec/plan and may go straight to a branch and PR. Dependency PRs use
the merge-bot skill.

1. Brainstorm the design with `superpowers:brainstorming` → spec in
   `docs/superpowers/specs/`.
2. Write the implementation plan with `superpowers:writing-plans` → plan in
   `docs/superpowers/plans/`.
3. Create a Linear issue on the **Engineering** team.
4. Branch off `main` using the branch name Linear suggests — branch *before* the
   spec and plan are committed, so design docs land on the feature branch.
5. Implement with `superpowers:subagent-driven-development` (per-task TDD plus
   spec/quality review, then a final whole-branch review).
6. `make lint` and `make test` must pass (see CONTRIBUTING.md).
7. Open a PR referencing the Linear issue; include a `Co-Authored-By` trailer on
   agent commits.
8. Address CodeRabbit comments with `superpowers:receiving-code-review` — verify
   each before applying; reply in the thread.
9. A human reviews and squash-merges. The agent never merges.
10. After merge, set the Linear issue to Done and delete the merged branch.

## Architecture

### Module Structure

The repository uses a multi-module Go workspace:

- Root module: `go.olly.garden/grafts` (placeholder)
- Component modules: each component (e.g. `receiver/natsjetstreamreceiver`) is a
  separate Go module with its own `go.mod`.

This structure is required by OCB, which references components as separate
modules with `path:` for local development and `replaces:` directives.

### Distribution

The `distributions/grafts/` directory contains:

- `manifest.yaml`: OCB manifest defining included components (receivers,
  processors, exporters, extensions, connectors, providers)
- `config.yaml`: sample collector configuration
- `Makefile`: build automation using the `builder` CLI

OCB generates the collector binary in `distributions/grafts/build/grafts`. To
run it: `cd distributions/grafts && make run` (or `make validate`).

### Components

**NATS JetStream Receiver** (`receiver/natsjetstreamreceiver/`):

- Consumes traces, metrics, and logs from NATS JetStream using pull-based consumers
- Uses a shared receiver pattern (single NATS connection for all signal types)
- Expects OTLP protobuf format on configured subjects
- Supports JetStream domains for clustered NATS deployments

Key files: `config.go` (config + validation), `factory.go` (shared instance via
`sync.Once`), `receiver.go` (two-phase init with graceful shutdown).

**NATS JetStream Exporter** (`exporter/natsjetstreamexporter/`):

- Publishes traces, metrics, and logs to NATS JetStream streams
- Sync and async publishing modes for throughput/reliability trade-offs
- OTLP protobuf format, compatible with the receiver above
- Supports JetStream domains for clustered NATS deployments

Key files: `config.go`, `factory.go`, `exporter.go` (publishing with sync/async
modes and error classification).

**Parquet Exporter** (`exporter/parquetexporter/`):

- Writes traces, metrics, and logs to local Parquet files for DuckDB consumption
- Pure Go (no CGo) via apache/arrow-go; DuckDB reads via `read_parquet()`
- Schema mirrors the ClickHouse exporter: traces (+events/links), logs, and five
  metric files (gauge/sum/histogram/exponential_histogram/summary)
- Attribute maps stored as JSON strings; files rotate on time/rows/bytes with
  atomic `.part` → `.parquet` rename
- Optional encryption at rest (Parquet Modular Encryption, AES-GCM)
- Emits its own metrics (`parquetexporter.*`) for rotation, rows/bytes, and I/O
  errors

Key files: `config.go`, `telemetry.go` (self-telemetry + error classification),
`schema.go` (Arrow schemas), `writer.go` (rotating writer with atomic rename),
`traces.go`/`logs.go`/`metrics.go` (OTLP → Arrow transforms), `exporter.go`
(lifecycle, flush ticker, push methods).

**SNMP Receiver** (`receiver/snmpreceiver/`):

- Polls SNMP targets for metrics and listens for traps/informs as logs
- Supports SNMPv2c and SNMPv3 with named, reusable auth configurations
- Metric groups define OID collections with table walks, index extraction, and
  lookup chains
- Trap listener converts SNMP traps to OTel log records with severity mapping
- Uses `gosnmp/gosnmp` (pure Go, no CGo)

Key files: `config.go`, `factory.go` (shared instance for metrics + logs),
`receiver.go` (orchestrator), `internal/connection/` (gosnmp wrapper + mock),
`internal/poller/` (scheduler + collector), `internal/trapper/` (UDP trap
listener), `internal/metrics/` (pmetric builder), `internal/logs/` (plog builder).

## Configuration

**NATS JetStream Receiver** requires: `url`, `stream`, `consumer_name`, `domain`,
and `subjects.traces/metrics/logs`.

**NATS JetStream Exporter** requires: `url`, `stream`, `domain`,
`subjects.traces/metrics/logs`, and `publish_async` (default: true).

**Parquet Exporter** requires `directory`; optional `flush_interval` (5m),
`max_rows` (100000), `max_bytes` (128000000), `compression` (zstd/snappy/none),
and `encryption` (base64 AES key + optional `key_id`).

**SNMP Receiver** requires: `auth` (named v2c/v3 configs), `targets`,
`metric_groups`, optional `trap_listener`, `collection_interval` (60s), `timeout`
(5s).
