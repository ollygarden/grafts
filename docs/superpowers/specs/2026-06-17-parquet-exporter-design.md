# Parquet Exporter Design

**Date:** 2026-06-17
**Status:** Approved
**Component:** `exporter/parquetexporter` (type `parquet`)

## Summary

A pure-Go OpenTelemetry Collector exporter that writes traces, metrics, and logs
to Parquet files on local disk, with a schema designed for DuckDB to query via
`read_parquet()`. No CGo.

## Motivation

There is no pure-Go DuckDB engine — every in-process path to the DuckDB engine
requires CGo (`github.com/duckdb/duckdb-go/v2`), which forces `CGO_ENABLED=1`,
a fat binary, and painful cross-compilation for a collector distribution. The
pure-Go alternative is to write Parquet files that DuckDB reads externally via
`read_parquet()`.

There is also a genuine gap: contrib's `parquetexporter` was only ever a ~37-line
stub and was removed in Oct 2023 (PR #27285, fixing #27284). The `awss3exporter`
supports only `otlp_proto`, `otlp_json`, `sumo_ic`, and `body` marshalers — no
Parquet — and there is no Parquet encoding extension in contrib. So an
OTel → Parquet (for DuckDB) exporter is not redundant.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Write target | Parquet via `apache/arrow-go/v18` | Pure Go, no CGo, static binary |
| Destination | Local filesystem only (v1) | Simplest, fewest deps; object store can come later |
| File layout | Flat per-signal directories | Simpler than Hive partitioning for v1 |
| Attributes | JSON string columns (`VARCHAR`) | Preserves value types; simple Arrow schema; `json_extract()` in DuckDB |
| Rotation | Time + size + rows, whichever first | Bounds both latency-to-queryable and file size |
| Name | `parquet` (`exporter/parquetexporter`) | Accurately describes output; revives the name contrib dropped |
| Schema | Mirror ClickHouse exporter shape | Proven OTel-on-columnar layout; 5 metric files |

## Architecture

Follows the same conventions as `natsjetstreamexporter`:

- `factory.go` — `NewFactory()` with `exporter.WithTraces/WithMetrics/WithLogs`,
  all `component.StabilityLevelAlpha`; `createDefaultConfig()`.
- `config.go` — `Config` struct + `Validate()`.
- `exporter.go` — lifecycle (`Start`/`Shutdown`) and per-signal writers.
- Standard `_test.go` files, `doc.go`, `README.md`.

Each signal type has an independent **writer** that buffers incoming rows into an
open Parquet file and rotates on policy. `Shutdown` flushes and closes all open
writers so no data is lost on stop.

### File layout

```text
<directory>/
  traces/part-<unixnano>-<seq>.parquet
  logs/part-<unixnano>-<seq>.parquet
  metrics_gauge/part-<unixnano>-<seq>.parquet
  metrics_sum/...
  metrics_histogram/...
  metrics_exponential_histogram/...
  metrics_summary/...
```

Files are written to a `.part` temp name and atomically renamed to `.parquet`
on close, so DuckDB never reads a half-written file.

### Rotation

The open file rotates when **any** threshold is hit:

- `flush_interval` (default `5m`)
- `max_rows` (default `100000`)
- `max_bytes` (default `128000000`)

## Schema

Mirrors the ClickHouse exporter; all attribute maps stored as JSON `VARCHAR`.

### traces

Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind,
ServiceName, ResourceAttributes (JSON), ScopeName, ScopeVersion,
SpanAttributes (JSON), Duration (ns), StatusCode, StatusMessage,
Events `LIST(STRUCT(Timestamp, Name, Attributes JSON))`,
Links `LIST(STRUCT(TraceId, SpanId, TraceState, Attributes JSON))`.

### logs

Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber,
ServiceName, Body, ResourceAttributes (JSON), ScopeName, ScopeVersion,
ScopeAttributes (JSON), LogAttributes (JSON), EventName.

### metrics (5 files)

Shared columns (all five): ResourceAttributes (JSON), ResourceSchemaUrl,
ScopeName, ScopeVersion, ScopeAttributes (JSON), ScopeSchemaUrl, ServiceName,
MetricName, MetricDescription, MetricUnit, Attributes (JSON), StartTimeUnix,
TimeUnix, Flags.

Type-specific:

- **gauge** — Value (Float64)
- **sum** — Value, AggregationTemporality (Int32), IsMonotonic (Bool)
- **histogram** — Count, Sum, BucketCounts (LIST), ExplicitBounds (LIST), Min, Max, AggregationTemporality
- **exponential_histogram** — Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts (LIST), NegativeOffset, NegativeBucketCounts (LIST), Min, Max, AggregationTemporality
- **summary** — Count, Sum, ValueAtQuantiles `LIST(STRUCT(Quantile, Value))`

Exemplars stored as `LIST(STRUCT(FilteredAttributes JSON, TimeUnix, Value, SpanId, TraceId))`
on the four non-summary metric types.

## Configuration

```yaml
exporters:
  parquet:
    directory: /var/lib/otel/parquet   # required
    flush_interval: 5m
    max_rows: 100000
    max_bytes: 128000000
    compression: zstd                  # zstd|snappy|none (default zstd)
```

`Validate()`: `directory` required; `flush_interval`, `max_rows`, `max_bytes` > 0;
`compression` in {zstd, snappy, none}.

## Error handling

- Write/rotate failures are returned to the collector pipeline so
  `exporterhelper` retry/queue applies.
- Disk-full and permission errors are surfaced clearly.
- `Shutdown` flushes all writers; a flush error on shutdown is logged and returned.

## Testing

- Per-writer unit tests: schema correctness, rotation triggers (time, rows,
  bytes), JSON attribute encoding, atomic rename.
- Config validation tests.
- Factory tests (matching existing `_test.go` pattern).
- Round-trip test: write Parquet, read it back with arrow-go, assert column values.

## Wiring

- Add test + lint targets to root `Makefile`.
- Add `gomod`/`import`/`path` entry to `distributions/grafts/manifest.yaml`.
- Document in `CLAUDE.md` (repo ai-context): Components section + Configuration section.

## Out of scope (v1)

- Object storage / S3 destinations (write to local dir only).
- Hive partitioning by date.
- Live `.duckdb` file output (CGo).
- Background compaction of small files.
