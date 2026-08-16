# Grafts repository guide

Grafts is OllyGarden's public collection of custom OpenTelemetry Collector
components. Components are grafted into a Collector distribution with the
OpenTelemetry Collector Builder (OCB).

Read [CONTRIBUTING.md](CONTRIBUTING.md) for contributor setup, commit, and
pull-request expectations. This guide is the codebase map and change-specific
validation reference.

## Development commands

Use Go 1.26.6 to match CI. The root module declares Go 1.26.6 compatibility. The
distribution build also requires the OCB `builder`; its Makefile installs the
version pinned by `OCB_VERSION` when `builder` is not already available.

```bash
make tidy
git diff --exit-code -- go.mod go.sum
make build
make lint
make test
make test-integration
git diff --check
test -z "${BASE_SHA:-}" || git diff --check "${BASE_SHA}...HEAD"
```

`make test-integration` exercises the SNMP receiver with Docker-backed
dependencies and may skip tests when Docker is unavailable. `make fmt` and
`make tidy` modify files; review their complete diff. For a focused test:

```bash
go test -v ./receiver/natsjetstreamreceiver/... -run TestName
```

## Development workflow

Substantial feature or behavior changes use the repository's established
design workflow:

1. Brainstorm the design (with `superpowers:brainstorming` when available) and
   record a specification in
   `docs/superpowers/specs/`.
2. Record the implementation plan (with `superpowers:writing-plans` when
   available) in `docs/superpowers/plans/`.
3. Create or link the appropriate issue. OllyGarden employees use the
   Engineering team in Linear; external contributors should start with a
   GitHub issue unless a maintainer directs otherwise.
4. Branch from current `main` before committing the design documents.
   OllyGarden employees use the branch name suggested by Linear.
5. Implement with per-task tests and complete both focused and whole-branch
   review. The established agent workflow uses
   `superpowers:subagent-driven-development` when available.
6. Run the relevant validation matrix below.
7. Open a focused pull request and disclose risks, compatibility effects, and
   exact validation results. Agent-authored commits include the appropriate
   `Co-Authored-By` trailer.
8. Verify CodeRabbit findings before applying them and reply on each thread;
   use `superpowers:receiving-code-review` when available.
9. A human reviewer squash-merges. Coding agents do not merge.
10. OllyGarden employees mark the linked Linear issue Done and delete the
    merged branch after merge.

Typos, comments, and small localized fixes may skip the design documents.
Renovate owns routine dependency pull requests; maintainers may use their
merge-bot workflow for those PRs.

## Architecture

### Module and distribution

The repository has one Go module, `go.olly.garden/grafts`, rooted at
`go.mod`. Receivers and exporters are packages in that module; do not add
component-level modules.

`distributions/grafts/` contains:

- `manifest.yaml`, the OCB manifest;
- `config.yaml`, a sample Collector configuration;
- `Makefile`, distribution build and validation commands.

The manifest references the root module with
`gomod: go.olly.garden/grafts`, selects each component with its nested
`import:` path, and uses the root through `path:` and `replaces:` during
local builds.
Preserve that combination. OCB writes generated artifacts under
`distributions/grafts/build/`; they are ignored and must not be committed.

Run the generated Collector with `make -C distributions/grafts run`, or
validate the sample configuration without starting pipelines:

```bash
make -C distributions/grafts validate
```

### Components

#### NATS JetStream receiver

`receiver/natsjetstreamreceiver/` consumes traces, metrics, and logs from
pull-based JetStream consumers. It shares one NATS connection across signal
types, expects OTLP protobuf payloads, supports JetStream domains, and
propagates trace context.

Key files are `config.go` for configuration and validation, `factory.go`
for the `sync.Once` shared instance, `receiver.go` for two-phase
initialization, delivery, and graceful shutdown, and `telemetry.go` for
self-observability.

#### NATS JetStream exporter

`exporter/natsjetstreamexporter/` publishes OTLP protobuf traces, metrics, and
logs to JetStream. It supports synchronous and asynchronous publishing and
classifies publish failures for Collector error handling and telemetry.

Key files are `config.go`, `factory.go`, `exporter.go`, and
`telemetry.go`.

#### Parquet exporter

`exporter/parquetexporter/` writes local Parquet files for DuckDB without
CGo through `apache/arrow-go`. Its schema mirrors the ClickHouse exporter:
traces with events and links, logs, and gauge, sum, histogram, exponential
histogram, and summary metric files. It stores attribute maps as JSON strings;
rotates by time, rows, or bytes using atomic `.part`-to-`.parquet` renames;
optionally uses Parquet Modular Encryption with AES-GCM; and emits
`parquetexporter.*` metrics for rotation, rows, bytes, and I/O errors.

Key files are `schema.go`, `writer.go`, `traces.go`, `logs.go`,
`metrics.go`, `exporter.go`, and `telemetry.go`.

#### SNMP receiver

`receiver/snmpreceiver/` polls SNMP targets for metrics and receives
traps/informs as logs. It supports SNMPv2c and SNMPv3, reusable authentication
configurations, metric groups, table walks, index extraction, lookup chains,
and severity mapping. It uses the pure-Go `gosnmp/gosnmp` library.

Key files are `config.go`, `factory.go`, and `receiver.go`.
`internal/connection/`, `internal/poller/`, `internal/trapper/`,
`internal/metrics/`, and `internal/logs/` contain the protocol and signal
implementations.

## Configuration

- The NATS receiver requires `url`, `stream`, `consumer_name`, and at least one
  signal subject. `domain` is optional for clustered JetStream deployments;
  acknowledgement and connection settings have validated defaults.
- The NATS exporter requires `url`, `stream`, and at least one signal subject.
  `domain` is optional, `publish_async` defaults to `true`, and
  `flush_timeout` defaults to 5 seconds.
- The Parquet exporter requires `directory`. Defaults are
  `flush_interval: 5m`, `max_rows: 100000`, and
  `max_bytes: 128000000`; compression accepts `zstd`, `snappy`, or
  `none`. Encryption accepts a base64 AES key and optional `key_id`. Never
  commit real encryption keys.
- The SNMP receiver requires at least one polling `target` or a
  `trap_listener`. Polling targets reference named `auth` and `metric_groups`;
  trap-only configurations need neither when no accepted auth is configured.
  The collection interval defaults to 60 seconds and timeout to 5 seconds.

Keep component README examples, factory defaults, configuration validation,
and `distributions/grafts/config.yaml` aligned when a configuration contract
changes.

## Conventions and guardrails

- Follow Collector component lifecycle and factory patterns. Use Collector nop
  test settings for wiring and keep shared receiver/exporter instances safe
  across signals.
- Preserve NATS acknowledgement, retry, error classification, sync/async
  semantics, and trace propagation. Test failure and shutdown paths, not only
  successful delivery.
- Keep components portable and prefer pure Go. Integration tests may use real
  embedded or containerized dependencies; unit tests should not require them.
- Use `require` for test preconditions and fatal assertions, `assert` for
  non-fatal checks, and table-driven subtests where useful.
- Follow OpenTelemetry semantic conventions for component telemetry. Preserve
  bounded-cardinality attributes and `error.type` classification; never emit
  payloads, credentials, community-submitted data, or encryption keys.
- Parquet writes must retain atomic rotation and explicit flush/shutdown
  behavior. Treat schema and encryption changes as compatibility-sensitive.
- Do not edit generated files under `distributions/grafts/build/` or commit
  build outputs.

## Validation matrix

| Change | Required validation |
| --- | --- |
| Documentation only | `git diff --check` and link/config example review |
| Go source | `make tidy`, `make lint`, and `make test` |
| Component configuration or factory | Go checks plus focused default, validation, and lifecycle tests |
| NATS receiver/exporter | Go checks plus focused delivery, ack/retry, async, and propagation tests |
| Parquet schema/writer/encryption | Go checks plus focused rotation, flush, compatibility, and encryption tests |
| SNMP behavior | Go checks plus focused unit tests and `make test-integration` |
| OCB manifest or distribution config | `make build` and `make -C distributions/grafts validate` |
| Dependencies | Full CI-equivalent gate, distribution build, and tidy diff |
