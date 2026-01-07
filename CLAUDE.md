# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Grafts is a collection of custom OpenTelemetry Collector components for OllyGarden. Components are "grafted" onto the standard collector via the OpenTelemetry Collector Builder (OCB).

## Build Commands

From the repository root:
```bash
make test       # Run tests for all components
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
