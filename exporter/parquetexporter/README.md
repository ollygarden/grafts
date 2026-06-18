# Parquet Exporter

Writes traces, metrics, and logs to local Parquet files with a schema designed
for DuckDB. Pure Go (no CGo) via apache/arrow-go.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `directory` | (required) | Root directory for per-signal subdirectories. |
| `flush_interval` | `5m` | Rotate the open file once it reaches this age. |
| `max_rows` | `100000` | Rotate once the open file holds this many rows. |
| `max_bytes` | `128000000` | Rotate once the open file reaches this size on disk. |
| `compression` | `zstd` | Column compression: `zstd`, `snappy`, or `none`. |

```yaml
exporters:
  parquet:
    directory: /var/lib/otel/parquet
    flush_interval: 5m
    max_rows: 100000
    max_bytes: 128000000
    compression: zstd
```

## Layout

Files are written per signal: `traces/`, `logs/`, and one directory per metric
type (`metrics_gauge`, `metrics_sum`, `metrics_histogram`,
`metrics_exponential_histogram`, `metrics_summary`). Each file is written as
`part-<unixnano>-<seq>.parquet.part` and atomically renamed to `.parquet` on
close, so readers never see a partial file.

## Querying with DuckDB

```sql
SELECT ServiceName, SpanName, count(*)
FROM read_parquet('/var/lib/otel/parquet/traces/*.parquet')
GROUP BY 1, 2;

-- attributes are JSON strings:
SELECT json_extract_string(SpanAttributes, '$."http.method"') AS method, count(*)
FROM read_parquet('/var/lib/otel/parquet/traces/*.parquet')
GROUP BY 1;
```

## Observability

The exporter emits its own metrics (scope `go.olly.garden/grafts/exporter/parquetexporter`)
in addition to the collector's standard `otelcol_exporter_*` counters:

| Metric | Description | Attributes |
|--------|-------------|------------|
| `parquetexporter.files.rotated` | Files closed and renamed into place | `parquet.table`, `parquet.rotation.reason` (`rows`/`bytes`/`age`/`shutdown`) |
| `parquetexporter.rows.written` | Rows committed (at rotation) | `parquet.table` |
| `parquetexporter.bytes.written` | Bytes committed (at rotation) | `parquet.table` |
| `parquetexporter.rotation.duration` | Successful rotation latency (close+fsync+rename) | `parquet.table` |
| `parquetexporter.errors` | File I/O errors | `parquet.table`, `parquet.operation` (`create`/`write`/`sync`/`rename`), `error.type` (`disk_full`/`permission`/`io`) |

I/O failures are also logged at `ERROR` with the offending file path. A failed
rename leaves an orphan `.part` file — the log names it so it can be cleaned up.

## Notes

- Timestamps are stored as unix-nanosecond `BIGINT`; convert with
  `make_timestamp_ns(TimeUnix)` in DuckDB.
- v1 writes to local disk only. Object storage, Hive partitioning, and live
  `.duckdb` output are out of scope.
