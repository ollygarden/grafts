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

### Encryption at rest (optional)

When the `encryption` block is set, files are written with Parquet Modular
Encryption (AES-GCM) — footer and all columns are encrypted. Omit the block to
write plaintext Parquet (the default).

```yaml
exporters:
  parquet:
    directory: /data/parquet
    encryption:
      key: ${env:PARQUET_ENCRYPTION_KEY}  # base64-encoded raw AES key, 16/24/32 bytes (AES-128/192/256)
      key_id: "key1"                       # optional label, stored as footer-key metadata (never the key)
```

The key is read from config at startup and held only in memory; it is never
written to the Parquet files. A bad key (not base64, or not 16/24/32 bytes) fails
collector startup.

#### Reading encrypted files from DuckDB

DuckDB reads Parquet Modular Encryption natively. Register the key, then pass an
`encryption_config`:

```sql
PRAGMA add_parquet_key('key1', '0123456789abcdef');
SELECT * FROM read_parquet('traces/*.parquet',
                           encryption_config = {footer_key: 'key1'});
```

**Key encoding:** the value passed to `add_parquet_key` must be the *raw key
bytes* the exporter was given — that is, the bytes your base64 `key` decodes to,
passed as a literal string. For example, if your base64 `key` is
`MDEyMzQ1Njc4OWFiY2RlZg==`, the raw bytes it decodes to are the 16-character
ASCII string `0123456789abcdef`, and that is exactly what you pass to
`add_parquet_key`. Do **not** pass the base64 form to DuckDB.

This was verified with DuckDB v1.5.1 using a 16-byte ASCII key:

```bash
$ duckdb -c "PRAGMA add_parquet_key('key1', '0123456789abcdef'); \
             SELECT count(*) FROM read_parquet('/tmp/enc.parquet', \
             encryption_config = {footer_key: 'key1'});"
# Output: count_star() = 3  (matches the 3 rows written by the exporter)
```

Verify the round trip on your DuckDB version before relying on it in production.

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
