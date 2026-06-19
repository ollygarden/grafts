# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Grafts is a collection of custom OpenTelemetry Collector components for OllyGarden. Components are "grafted" onto the standard collector via the OpenTelemetry Collector Builder (OCB).

## Build Commands

From the repository root:
```bash
make test              # Run tests for all components
make test-integration  # Run Docker-backed integration tests (snmpreceiver)
make lint       # Run linter for all components
make fmt        # Format all components
make tidy       # Run go mod tidy for all components
make build      # Build the test distribution
```

To run the test distribution:
```bash
cd distributions/grafts
make run        # Build and run with config.yaml
make validate   # Validate configuration
```

Run a specific test:
```bash
cd receiver/natsjetstreamreceiver
go test -v ./... -run TestName
```

### Before committing or pushing

Run `make lint` (or `golangci-lint run ./<receiver|exporter>/<component>/...` for a
single component). CI gates on it, and `go vet`/`go test` are **not** a substitute:
golangci-lint additionally runs `errcheck` (unchecked error returns, e.g. a bare
`defer f.Close()`) and `staticcheck` (including `SA1019` deprecated-API usage). Both
have slipped past `go vet`/`go test` before — lint is the source of truth.

## Architecture

### Module Structure

The repository uses a multi-module Go workspace:
- Root module: `go.olly.garden/grafts` (placeholder)
- Component modules: Each component (e.g., `receiver/natsjetstreamreceiver`) is a separate Go module with its own `go.mod`

This structure is required by OCB, which references components as separate modules with `path:` for local development and `replaces:` directives.

### Distribution

The `distributions/grafts/` directory contains:
- `manifest.yaml`: OCB manifest defining included components (receivers, processors, exporters, extensions, connectors, providers)
- `config.yaml`: Sample collector configuration
- `Makefile`: Build automation using `builder` CLI

OCB generates the collector binary in `distributions/grafts/build/grafts`.

### Components

**NATS JetStream Receiver** (`receiver/natsjetstreamreceiver/`):
- Consumes traces, metrics, and logs from NATS JetStream using pull-based consumers
- Uses shared receiver pattern (single NATS connection for all signal types)
- Expects OTLP protobuf format on configured subjects
- Supports JetStream domains for clustered NATS deployments

Key files:
- `config.go`: Configuration struct with validation
- `factory.go`: Receiver factory with shared instance management via `sync.Once`
- `receiver.go`: Two-phase initialization (create consumers, then start loops) with graceful shutdown

**NATS JetStream Exporter** (`exporter/natsjetstreamexporter/`):
- Publishes traces, metrics, and logs to NATS JetStream streams
- Supports sync and async publishing modes for throughput/reliability trade-offs
- OTLP protobuf format compatible with natsjetstreamreceiver
- Supports JetStream domains for clustered NATS deployments

Key files:
- `config.go`: Configuration struct with validation
- `factory.go`: Exporter factory
- `exporter.go`: Publishing logic with sync/async modes and error classification

**Parquet Exporter** (`exporter/parquetexporter/`):
- Writes traces, metrics, and logs to local Parquet files for DuckDB consumption
- Pure Go (no CGo) via apache/arrow-go; DuckDB reads files via read_parquet()
- Schema mirrors the ClickHouse exporter: traces (+events/links), logs, and five
  metric files (gauge/sum/histogram/exponential_histogram/summary)
- Attribute maps stored as JSON strings; files rotate on time/rows/bytes with
  atomic .part -> .parquet rename
- Emits own metrics (parquetexporter.*) for rotation, rows/bytes, and I/O
  errors (by operation + error.type); failures logged with the file path

Key files:
- `config.go`: Configuration struct with validation
- `telemetry.go`: Self-telemetry instruments (rotation, errors) + error classification
- `schema.go`: Arrow schemas for all signal tables
- `writer.go`: Rotating Parquet writer with atomic rename + telemetry recording
- `traces.go`/`logs.go`/`metrics.go`: OTLP -> Arrow record transforms
- `exporter.go`: Lifecycle, background flush ticker, push methods

**SNMP Receiver** (`receiver/snmpreceiver/`):
- Polls SNMP targets for metrics and listens for traps/informs as logs
- Supports SNMPv2c and SNMPv3 with named, reusable auth configurations
- Metric groups define OID collections with table walks, index extraction, and lookup chains
- Trap listener converts SNMP traps to OTel log records with severity mapping
- Uses `gosnmp/gosnmp` (pure Go, no CGo)

Key files:
- `config.go`: Configuration structs (auth, targets, metric groups, trap listener) with validation
- `factory.go`: Receiver factory with shared instance management (metrics + logs signals)
- `receiver.go`: Orchestrator wiring poller and trapper into lifecycle
- `internal/connection/`: Connection interface wrapping gosnmp + mock for testing
- `internal/poller/`: Poll scheduler (per-target goroutines) + metric group collector (GET/WALK)
- `internal/trapper/`: UDP trap listener with auth filtering
- `internal/metrics/`: pmetric builder (SNMP responses -> OTel metrics)
- `internal/logs/`: plog builder (trap PDUs -> OTel logs)

## Configuration

**NATS JetStream Receiver** requires:
- `url`: NATS server URL
- `stream`: JetStream stream name (must exist)
- `consumer_name`: Durable consumer name prefix
- `domain`: JetStream domain (required for clustered NATS deployments)
- `subjects.traces/metrics/logs`: Subject patterns per signal type

**NATS JetStream Exporter** requires:
- `url`: NATS server URL
- `stream`: JetStream stream name (must exist)
- `domain`: JetStream domain (required for clustered NATS deployments)
- `subjects.traces/metrics/logs`: Subject patterns per signal type
- `publish_async`: Whether to use async publishing (default: true)

**Parquet Exporter** requires:
- `directory`: Root directory for Parquet output (required)
- `flush_interval`: Max age before rotating the open file (default 5m)
- `max_rows`: Max rows before rotating (default 100000)
- `max_bytes`: Max file size before rotating (default 128000000)
- `compression`: Column compression — zstd, snappy, or none (default zstd)

**SNMP Receiver** requires:
- `auth`: Named auth configurations (`v2c` with community, or `v3` with USM credentials)
- `targets`: List of SNMP devices with host, auth reference, and metric group references
- `metric_groups`: Named OID collections with metrics (oid, metric_name, type), attributes, lookups
- `trap_listener` (optional): UDP listen address and accepted auth list
- `collection_interval`: Polling interval (default: 60s)
- `timeout`: SNMP request timeout (default: 5s)
