# Parquet Exporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure-Go OpenTelemetry Collector exporter that writes traces, metrics, and logs to local Parquet files with a DuckDB-friendly schema.

**Architecture:** A factory exposes traces/metrics/logs exporters (alpha). Each signal has an independent `signalWriter` that appends incoming OTLP batches as Parquet row groups to an open file under a per-signal subdirectory, rotating the file on time/rows/bytes thresholds with an atomic `.part` → `.parquet` rename. Attribute maps are serialized to JSON `VARCHAR` columns. Metrics are split into five files by type (gauge/sum/histogram/exponential_histogram/summary), mirroring the ClickHouse exporter.

**Tech Stack:** Go, `go.opentelemetry.io/collector` exporter framework + `exporterhelper`, `github.com/apache/arrow-go/v18` (arrow + parquet/pqarrow), no CGo.

## Global Constraints

- Module: `go.olly.garden/grafts` (single root module; component lives at `exporter/parquetexporter`, package `parquetexporter`).
- Component type string: `parquet`. Stability: `component.StabilityLevelAlpha` for all three signals.
- No CGo. Only pure-Go dependencies (`apache/arrow-go/v18`).
- Attribute maps stored as JSON strings via `json.Marshal(m.AsRaw())`.
- Timestamps stored as `arrow.PrimitiveTypes.Int64` unix-nanoseconds (column type `BIGINT` in DuckDB) for exact parity; DuckDB converts with `make_timestamp_ns()` when needed.
- Each OTLP batch is written as exactly one Parquet row group (`fw.Write(rec)`), so on-disk file size after each write is accurate via `os.File.Stat().Size()`.
- Follow the conventions of `exporter/natsjetstreamexporter` (factory shape, `Config.Validate()`, `Start`/`Shutdown` lifecycle, `_test.go` layout, `doc.go`, `README.md`).
- Tests use `github.com/stretchr/testify` (already at `v1.11.1` in the repo): `require` for fatal preconditions (setup, "must not error before we can assert"), `assert` for value checks. No hand-rolled assertion helpers.
- Component self-telemetry: metrics via the meter from `set.TelemetrySettings.MeterProvider` (scope `go.olly.garden/grafts/exporter/parquetexporter`); diagnostic logs via the `set.Logger` (zap). Do NOT duplicate `exporterhelper`'s generic `otelcol_exporter_sent_*` / `otelcol_exporter_send_failed_*`; custom metrics cover file rotation and I/O failure only. No custom spans (the `exporterhelper` export span owns the push boundary; age-triggered rotations have no trace context). All metric attribute values are bounded — file paths appear in logs only.
- Run `make fmt` before each commit; commits are frequent and scoped per task.

## File Structure

All under `exporter/parquetexporter/`:

- `doc.go` — package doc + import path comment.
- `config.go` — `Config`, `Compression` constants, `Validate()`, `createDefaultConfig()`.
- `config_test.go` — default + validation tests.
- `telemetry.go` — `telemetry` instruments struct, `newTelemetry`, `recordRotation`/`recordError`, `classifyError`, scope/reason/operation consts.
- `telemetry_test.go` — instrument emission + error-classification tests.
- `factory.go` — `NewFactory()` and the three signal creators.
- `factory_test.go` — factory creation tests.
- `writer.go` — `signalWriter` (open/rotate/close, atomic rename, threshold checks) and `newWriterProperties`.
- `writer_test.go` — rotation + atomic-rename + round-trip tests using a trivial schema.
- `attributes.go` — `attributesToJSON(pcommon.Map) string` + `anyValueToJSON` helper.
- `attributes_test.go` — JSON encoding tests.
- `schema.go` — arrow schemas for all seven tables (traces, logs, 5 metrics) + exemplar/event/link struct types.
- `traces.go` — `tracesToRecord(ptrace.Traces) arrow.Record`.
- `logs.go` — `logsToRecord(plog.Logs) arrow.Record`.
- `metrics.go` — `metricsToRecords(pmetric.Metrics) map[metricType]arrow.Record`.
- `exporter.go` — `parquetExporter` struct, `newParquetExporter`, `Start`, `Shutdown`, `pushTraces`, `pushMetrics`, `pushLogs`, background flush ticker.
- `*_test.go` for traces/logs/metrics/exporter as noted per task.
- `README.md` — user docs.

Repo-level changes: root `Makefile`, `distributions/grafts/manifest.yaml`, `CLAUDE.md`.

---

### Task 1: Module scaffolding, config, and validation

**Files:**
- Create: `exporter/parquetexporter/doc.go`
- Create: `exporter/parquetexporter/config.go`
- Create: `exporter/parquetexporter/config_test.go`

**Interfaces:**
- Produces: `Config` struct with fields `Directory string`, `FlushInterval time.Duration`, `MaxRows int64`, `MaxBytes int64`, `Compression string`; `func (cfg *Config) Validate() error`; `func createDefaultConfig() component.Config`. Compression constants `compressionZstd="zstd"`, `compressionSnappy="snappy"`, `compressionNone="none"`.

- [ ] **Step 1: Create `doc.go`**

```go
// Package parquetexporter exports traces, metrics, and logs to local Parquet
// files with a DuckDB-friendly schema.
//
// Import path: go.olly.garden/grafts/exporter/parquetexporter
package parquetexporter
```

- [ ] **Step 2: Write the failing config test**

Create `exporter/parquetexporter/config_test.go`:

```go
package parquetexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 5*time.Minute, cfg.FlushInterval)
	assert.Equal(t, int64(100000), cfg.MaxRows)
	assert.Equal(t, int64(128000000), cfg.MaxBytes)
	assert.Equal(t, compressionZstd, cfg.Compression)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) { c.Directory = "/tmp/p" }, false},
		{"missing directory", func(c *Config) { c.Directory = "" }, true},
		{"zero flush interval", func(c *Config) { c.Directory = "/tmp/p"; c.FlushInterval = 0 }, true},
		{"zero max rows", func(c *Config) { c.Directory = "/tmp/p"; c.MaxRows = 0 }, true},
		{"zero max bytes", func(c *Config) { c.Directory = "/tmp/p"; c.MaxBytes = 0 }, true},
		{"bad compression", func(c *Config) { c.Directory = "/tmp/p"; c.Compression = "lz4" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... 2>&1 | head`
Expected: FAIL — `undefined: createDefaultConfig` / `undefined: Config`.

- [ ] **Step 4: Write `config.go`**

```go
package parquetexporter

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	compressionZstd   = "zstd"
	compressionSnappy = "snappy"
	compressionNone   = "none"
)

// Config defines configuration for the Parquet exporter.
type Config struct {
	// Directory is the root directory under which per-signal subdirectories
	// and Parquet files are written. Required.
	Directory string `mapstructure:"directory"`

	// FlushInterval rotates (closes) the open file once it reaches this age.
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// MaxRows rotates the open file once it holds this many rows.
	MaxRows int64 `mapstructure:"max_rows"`

	// MaxBytes rotates the open file once it reaches this size on disk.
	MaxBytes int64 `mapstructure:"max_bytes"`

	// Compression is the Parquet column compression: zstd, snappy, or none.
	Compression string `mapstructure:"compression"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Directory == "" {
		return errors.New("directory is required")
	}
	if cfg.FlushInterval <= 0 {
		return errors.New("flush_interval must be positive")
	}
	if cfg.MaxRows <= 0 {
		return errors.New("max_rows must be positive")
	}
	if cfg.MaxBytes <= 0 {
		return errors.New("max_bytes must be positive")
	}
	switch cfg.Compression {
	case compressionZstd, compressionSnappy, compressionNone:
	default:
		return errors.New("compression must be one of: zstd, snappy, none")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		FlushInterval: 5 * time.Minute,
		MaxRows:       100000,
		MaxBytes:      128000000,
		Compression:   compressionZstd,
	}
}
```

- [ ] **Step 5: Initialize the module dependency and run tests**

Run:
```bash
cd exporter/parquetexporter
go get go.opentelemetry.io/collector/component@v1.58.0
go test ./... 2>&1 | tail
```
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
cd ../..
make fmt
git add exporter/parquetexporter/doc.go exporter/parquetexporter/config.go exporter/parquetexporter/config_test.go go.mod go.sum
git commit -m "feat(parquetexporter): add config and validation"
```

---

### Task 2: Attribute JSON serialization

**Files:**
- Create: `exporter/parquetexporter/attributes.go`
- Create: `exporter/parquetexporter/attributes_test.go`

**Interfaces:**
- Produces: `func attributesToJSON(m pcommon.Map) string` — returns a JSON object string of the map's raw values; returns `"{}"` for an empty map and never returns an error (marshal failures yield `"{}"`).

- [ ] **Step 1: Write the failing test**

Create `attributes_test.go`:

```go
package parquetexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestAttributesToJSON(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("http.method", "GET")
	m.PutInt("retries", 3)
	m.PutBool("ok", true)

	got := attributesToJSON(m)
	// Map ordering is non-deterministic; assert all fragments are present.
	assert.Contains(t, got, `"http.method":"GET"`)
	assert.Contains(t, got, `"retries":3`)
	assert.Contains(t, got, `"ok":true`)
}

func TestAttributesToJSONEmpty(t *testing.T) {
	assert.Equal(t, "{}", attributesToJSON(pcommon.NewMap()))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestAttributesToJSON 2>&1 | head`
Expected: FAIL — `undefined: attributesToJSON`.

- [ ] **Step 3: Write `attributes.go`**

```go
package parquetexporter

import (
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// attributesToJSON serializes an attribute map to a JSON object string.
// pcommon.Map.AsRaw() returns a map[string]any with Go-native scalar values
// and nested maps/slices, which json.Marshal handles directly. An empty map
// or any marshal failure yields "{}".
func attributesToJSON(m pcommon.Map) string {
	if m.Len() == 0 {
		return "{}"
	}
	b, err := json.Marshal(m.AsRaw())
	if err != nil {
		return "{}"
	}
	return string(b)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestAttributesToJSON -v 2>&1 | tail`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/attributes.go exporter/parquetexporter/attributes_test.go
git commit -m "feat(parquetexporter): add attribute JSON serialization"
```

---

### Task 3: Telemetry instruments

**Files:**
- Create: `exporter/parquetexporter/telemetry.go`
- Create: `exporter/parquetexporter/telemetry_test.go`

**Interfaces:**
- Produces:
  - `const scopeName = "go.olly.garden/grafts/exporter/parquetexporter"`
  - rotation-reason consts `reasonRows, reasonBytes, reasonAge, reasonShutdown` and operation consts `opCreate, opWrite, opSync, opRename` (string values).
  - `type telemetry struct { ... }` holding the five instruments.
  - `func newTelemetry(set component.TelemetrySettings) (*telemetry, error)`.
  - `func (t *telemetry) recordRotation(ctx context.Context, table, reason string, rows, bytes int64, seconds float64)`.
  - `func (t *telemetry) recordError(ctx context.Context, table, op string, err error)`.
  - `func classifyError(err error) string` → bounded `disk_full|permission|io`.

**Rationale:** `exporterhelper` already emits generic `otelcol_exporter_sent_*` / `otelcol_exporter_send_failed_*`. These instruments add what those cannot: which file table, why a file rotated, and at which I/O stage (and error class) a failure occurred. No spans — the export span from `exporterhelper` already owns the push boundary, and age-triggered rotations have no trace context.

- [ ] **Step 1: Write the failing test**

Create `telemetry_test.go`:

```go
package parquetexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordRotationEmitsFilesRotated(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	ts := componenttest.NewNopTelemetrySettings()
	ts.MeterProvider = mp

	tel, err := newTelemetry(ts)
	require.NoError(t, err)

	tel.recordRotation(context.Background(), "traces", reasonRows, 10, 2048, 0.25)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "parquetexporter.files.rotated" {
				found = true
				sum := m.Data.(metricdata.Sum[int64])
				require.Len(t, sum.DataPoints, 1)
				assert.Equal(t, int64(1), sum.DataPoints[0].Value)
			}
		}
	}
	assert.True(t, found, "parquetexporter.files.rotated not emitted")
}

func TestClassifyError(t *testing.T) {
	assert.Equal(t, "io", classifyError(assert.AnError))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run "TestRecordRotation|TestClassifyError" 2>&1 | head`
Expected: FAIL — `undefined: newTelemetry`.

- [ ] **Step 3: Write `telemetry.go`**

```go
package parquetexporter

import (
	"context"
	"errors"
	"io/fs"
	"syscall"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const scopeName = "go.olly.garden/grafts/exporter/parquetexporter"

const (
	reasonRows     = "rows"
	reasonBytes    = "bytes"
	reasonAge      = "age"
	reasonShutdown = "shutdown"
)

const (
	opCreate = "create"
	opWrite  = "write"
	opSync   = "sync"
	opRename = "rename"
)

type telemetry struct {
	filesRotated metric.Int64Counter
	rowsWritten  metric.Int64Counter
	bytesWritten metric.Int64Counter
	rotationDur  metric.Float64Histogram
	errors       metric.Int64Counter
}

func newTelemetry(set component.TelemetrySettings) (*telemetry, error) {
	m := set.MeterProvider.Meter(scopeName)
	t := &telemetry{}
	var errs error
	var err error

	if t.filesRotated, err = m.Int64Counter("parquetexporter.files.rotated",
		metric.WithUnit("{file}"),
		metric.WithDescription("Parquet files closed and atomically renamed into place.")); err != nil {
		errs = errors.Join(errs, err)
	}
	if t.rowsWritten, err = m.Int64Counter("parquetexporter.rows.written",
		metric.WithUnit("{row}"),
		metric.WithDescription("Rows committed to Parquet files (counted at rotation).")); err != nil {
		errs = errors.Join(errs, err)
	}
	if t.bytesWritten, err = m.Int64Counter("parquetexporter.bytes.written",
		metric.WithUnit("By"),
		metric.WithDescription("Bytes committed to Parquet files (counted at rotation).")); err != nil {
		errs = errors.Join(errs, err)
	}
	if t.rotationDur, err = m.Float64Histogram("parquetexporter.rotation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of a successful file rotation (close, fsync, rename).")); err != nil {
		errs = errors.Join(errs, err)
	}
	if t.errors, err = m.Int64Counter("parquetexporter.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("File I/O errors by operation and error class.")); err != nil {
		errs = errors.Join(errs, err)
	}
	return t, errs
}

func (t *telemetry) recordRotation(ctx context.Context, table, reason string, rows, bytes int64, seconds float64) {
	tableAttr := metric.WithAttributes(attribute.String("parquet.table", table))
	t.filesRotated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("parquet.table", table),
		attribute.String("parquet.rotation.reason", reason),
	))
	t.rowsWritten.Add(ctx, rows, tableAttr)
	t.bytesWritten.Add(ctx, bytes, tableAttr)
	t.rotationDur.Record(ctx, seconds, tableAttr)
}

func (t *telemetry) recordError(ctx context.Context, table, op string, err error) {
	t.errors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("parquet.table", table),
		attribute.String("parquet.operation", op),
		attribute.String("error.type", classifyError(err)),
	))
}

// classifyError maps an I/O error to a bounded error.type value so the errors
// counter stays low-cardinality.
func classifyError(err error) string {
	switch {
	case errors.Is(err, syscall.ENOSPC):
		return "disk_full"
	case errors.Is(err, fs.ErrPermission):
		return "permission"
	default:
		return "io"
	}
}
```

- [ ] **Step 4: Add the SDK metric test dependency and run tests**

Run:
```bash
cd exporter/parquetexporter
go get go.opentelemetry.io/otel/sdk/metric@latest
go test ./... -run "TestRecordRotation|TestClassifyError" -v 2>&1 | tail
```
Expected: PASS. (`go.opentelemetry.io/otel/metric` and `.../attribute` come transitively with the collector; align versions via `go mod tidy` if needed.)

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/telemetry.go exporter/parquetexporter/telemetry_test.go go.mod go.sum
git commit -m "feat(parquetexporter): add telemetry instruments"
```

---

### Task 4: Parquet writer core (rotation + atomic rename)

**Files:**
- Create: `exporter/parquetexporter/writer.go`
- Create: `exporter/parquetexporter/writer_test.go`

**Interfaces:**
- Consumes: `Config` (thresholds + compression), `*telemetry` and the `reason*`/`op*` consts (Task 3).
- Produces:
  - `func newWriterProperties(compression string) *parquet.WriterProperties`
  - `type signalWriter struct { ... }` — carries `table string` and `tel *telemetry` so all metrics are tagged with this writer's `parquet.table`.
  - `func newSignalWriter(table, dir string, schema *arrow.Schema, cfg *Config, tel *telemetry, logger *zap.Logger) (*signalWriter, error)` — creates `dir` (MkdirAll).
  - `func (w *signalWriter) write(rec arrow.Record) error` — appends one row group, rotates if thresholds exceeded (reason `rows` or `bytes`). Thread-safe.
  - `func (w *signalWriter) maybeRotateForAge() error` — rotates (reason `age`) only if an open file has reached `FlushInterval` age. Thread-safe.
  - `func (w *signalWriter) close() error` — finalizes any open file (reason `shutdown`, rename to `.parquet`). Thread-safe.
  - Recording happens inside the writer using `context.Background()` (component metrics need no request context); failures are also logged at Error via the zap logger with the file path.

- [ ] **Step 1: Write the failing test**

Create `writer_test.go`:

```go
package parquetexporter

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
)

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.BinaryTypes.String},
	}, nil)
}

func testTelemetry(t *testing.T) *telemetry {
	t.Helper()
	tel, err := newTelemetry(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	return tel
}

func oneRowRecord(t *testing.T, schema *arrow.Schema, val string) arrow.Record {
	t.Helper()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.StringBuilder).Append(val)
	return rb.NewRecord()
}

func countParquet(t *testing.T, dir string) int {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	return len(matches)
}

func TestWriterRotatesOnMaxRows(t *testing.T) {
	dir := t.TempDir()
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir
	cfg.MaxRows = 2
	cfg.MaxBytes = 1 << 30
	cfg.FlushInterval = time.Hour

	w, err := newSignalWriter("test", dir, testSchema(), cfg, testTelemetry(t), zap.NewNop())
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		rec := oneRowRecord(t, testSchema(), "x")
		require.NoError(t, w.write(rec))
		rec.Release()
	}
	require.NoError(t, w.close())

	// 5 rows, rotate every 2 -> files of 2,2,1 = 3 files. No .part remains.
	assert.Equal(t, 3, countParquet(t, dir))
	parts, _ := filepath.Glob(filepath.Join(dir, "*.part"))
	assert.Empty(t, parts, "no leftover .part files")
}

func TestWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir

	w, err := newSignalWriter("test", dir, testSchema(), cfg, testTelemetry(t), zap.NewNop())
	require.NoError(t, err)
	rec := oneRowRecord(t, testSchema(), "hello")
	require.NoError(t, w.write(rec))
	rec.Release()
	require.NoError(t, w.close())

	matches, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	require.Len(t, matches, 1)
	f, err := os.Open(matches[0])
	require.NoError(t, err)
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer rdr.Close()
	assert.Equal(t, int64(1), rdr.NumRows())
}

func TestWriterRotatesOnAge(t *testing.T) {
	dir := t.TempDir()
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir
	cfg.FlushInterval = time.Millisecond

	w, err := newSignalWriter("test", dir, testSchema(), cfg, testTelemetry(t), zap.NewNop())
	require.NoError(t, err)
	rec := oneRowRecord(t, testSchema(), "x")
	require.NoError(t, w.write(rec))
	rec.Release()
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, w.maybeRotateForAge())
	assert.Equal(t, 1, countParquet(t, dir))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestWriter 2>&1 | head`
Expected: FAIL — `undefined: newSignalWriter`.

- [ ] **Step 3: Write `writer.go`**

```go
package parquetexporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.uber.org/zap"
)

var seq atomic.Int64

func newWriterProperties(compression string) *parquet.WriterProperties {
	codec := compress.Codecs.Zstd
	switch compression {
	case compressionSnappy:
		codec = compress.Codecs.Snappy
	case compressionNone:
		codec = compress.Codecs.Uncompressed
	}
	return parquet.NewWriterProperties(parquet.WithCompression(codec))
}

// signalWriter owns a single open Parquet file for one signal table and
// rotates it based on row count, byte size, or age. All telemetry it records
// is tagged with its table name.
type signalWriter struct {
	table  string
	dir    string
	schema *arrow.Schema
	cfg    *Config
	props  *parquet.WriterProperties
	tel    *telemetry
	logger *zap.Logger

	mu       sync.Mutex
	file     *os.File
	fw       *pqarrow.FileWriter
	partPath string
	rows     int64
	openedAt time.Time
}

func newSignalWriter(table, dir string, schema *arrow.Schema, cfg *Config, tel *telemetry, logger *zap.Logger) (*signalWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", dir, err)
	}
	return &signalWriter{
		table:  table,
		dir:    dir,
		schema: schema,
		cfg:    cfg,
		props:  newWriterProperties(cfg.Compression),
		tel:    tel,
		logger: logger,
	}, nil
}

func (w *signalWriter) openLocked() error {
	name := fmt.Sprintf("part-%d-%d.parquet", time.Now().UnixNano(), seq.Add(1))
	w.partPath = filepath.Join(w.dir, name+".part")
	f, err := os.Create(w.partPath)
	if err != nil {
		w.tel.recordError(context.Background(), w.table, opCreate, err)
		w.logger.Error("parquet: create file failed", zap.String("path", w.partPath), zap.Error(err))
		return fmt.Errorf("create %s: %w", w.partPath, err)
	}
	fw, err := pqarrow.NewFileWriter(w.schema, f, w.props, pqarrow.DefaultWriterProps())
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("new parquet writer: %w", err)
	}
	w.file = f
	w.fw = fw
	w.rows = 0
	w.openedAt = time.Now()
	return nil
}

// rotateLocked closes the open writer and atomically renames .part -> .parquet,
// recording the outcome under the given reason.
func (w *signalWriter) rotateLocked(reason string) error {
	if w.fw == nil {
		return nil
	}
	ctx := context.Background()
	start := time.Now()
	rows := w.rows
	partPath := w.partPath

	if err := w.fw.Close(); err != nil {
		_ = w.file.Close()
		w.reset()
		w.tel.recordError(ctx, w.table, opWrite, err)
		w.logger.Error("parquet: close writer failed", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("close parquet writer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		w.reset()
		w.tel.recordError(ctx, w.table, opSync, err)
		w.logger.Error("parquet: fsync failed", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("sync: %w", err)
	}
	var size int64
	if info, serr := w.file.Stat(); serr == nil {
		size = info.Size()
	}
	if err := w.file.Close(); err != nil {
		w.reset()
		w.tel.recordError(ctx, w.table, opWrite, err)
		w.logger.Error("parquet: close file failed", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("close file: %w", err)
	}
	final := partPath[:len(partPath)-len(".part")]
	if err := os.Rename(partPath, final); err != nil {
		w.reset()
		w.tel.recordError(ctx, w.table, opRename, err)
		// The .part file is left behind on a failed rename — name it explicitly.
		w.logger.Error("parquet: rename failed, orphan .part left", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("rename %s: %w", partPath, err)
	}
	w.reset()
	w.tel.recordRotation(ctx, w.table, reason, rows, size, time.Since(start).Seconds())
	return nil
}

func (w *signalWriter) reset() {
	w.file = nil
	w.fw = nil
	w.partPath = ""
	w.rows = 0
}

func (w *signalWriter) write(rec arrow.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fw == nil {
		if err := w.openLocked(); err != nil {
			return err
		}
	}
	if err := w.fw.Write(rec); err != nil {
		w.tel.recordError(context.Background(), w.table, opWrite, err)
		w.logger.Error("parquet: write record failed", zap.String("path", w.partPath), zap.Error(err))
		return fmt.Errorf("write record: %w", err)
	}
	w.rows += rec.NumRows()

	var size int64
	if info, err := w.file.Stat(); err == nil {
		size = info.Size()
	}
	if w.rows >= w.cfg.MaxRows {
		return w.rotateLocked(reasonRows)
	}
	if size >= w.cfg.MaxBytes {
		return w.rotateLocked(reasonBytes)
	}
	return nil
}

func (w *signalWriter) maybeRotateForAge() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fw == nil {
		return nil
	}
	if time.Since(w.openedAt) >= w.cfg.FlushInterval {
		return w.rotateLocked(reasonAge)
	}
	return nil
}

func (w *signalWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked(reasonShutdown)
}
```

- [ ] **Step 4: Add arrow dependency and run tests**

Run:
```bash
cd exporter/parquetexporter
go get github.com/apache/arrow-go/v18@v18.6.0
go get go.uber.org/zap
go test ./... -run TestWriter -v 2>&1 | tail -20
```
Expected: PASS (3 writer tests).

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/writer.go exporter/parquetexporter/writer_test.go go.mod go.sum
git commit -m "feat(parquetexporter): add rotating parquet writer with atomic rename"
```

---

### Task 5: Schemas

**Files:**
- Create: `exporter/parquetexporter/schema.go`
- Create: `exporter/parquetexporter/schema_test.go`

**Interfaces:**
- Produces exported-within-package schema vars/functions:
  - `func tracesSchema() *arrow.Schema`
  - `func logsSchema() *arrow.Schema`
  - `func metricsGaugeSchema() *arrow.Schema`, `metricsSumSchema`, `metricsHistogramSchema`, `metricsExpHistogramSchema`, `metricsSummarySchema` — each `func() *arrow.Schema`.
  - Helper field constructors `jsonField(name)`, `tsField(name)`, `strField(name)`, `i64Field(name)`, `f64Field(name)`, `i32Field(name)`, `boolField(name)`, `f64ListField(name)`, `i64ListField(name)`.

- [ ] **Step 1: Write the failing test**

Create `schema_test.go`:

```go
package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestSchemasHaveExpectedColumns(t *testing.T) {
	hasField := func(t *testing.T, schema *arrow.Schema, name string) {
		t.Helper()
		_, ok := schema.FieldsByName(name)
		assert.True(t, ok, "schema missing field %q", name)
	}

	assert.NotZero(t, tracesSchema().NumFields(), "traces schema empty")
	hasField(t, tracesSchema(), "SpanAttributes")
	hasField(t, logsSchema(), "Body")
	hasField(t, metricsGaugeSchema(), "Value")
	hasField(t, metricsSumSchema(), "IsMonotonic")
	hasField(t, metricsHistogramSchema(), "BucketCounts")
	hasField(t, metricsExpHistogramSchema(), "PositiveBucketCounts")
	hasField(t, metricsSummarySchema(), "ValueAtQuantiles")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestSchemas 2>&1 | head`
Expected: FAIL — `undefined: tracesSchema`.

- [ ] **Step 3: Write `schema.go`**

```go
package parquetexporter

import "github.com/apache/arrow-go/v18/arrow"

func strField(name string) arrow.Field  { return arrow.Field{Name: name, Type: arrow.BinaryTypes.String} }
func jsonField(name string) arrow.Field { return arrow.Field{Name: name, Type: arrow.BinaryTypes.String} }
func tsField(name string) arrow.Field   { return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64} }
func i64Field(name string) arrow.Field  { return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64} }
func i32Field(name string) arrow.Field  { return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32} }
func f64Field(name string) arrow.Field  { return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64} }
func boolField(name string) arrow.Field { return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean} }

func f64ListField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)}
}
func i64ListField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}
}

// exemplarsType is LIST(STRUCT(FilteredAttributes, TimeUnix, Value, SpanId, TraceId)).
func exemplarsType() arrow.DataType {
	return arrow.ListOf(arrow.StructOf(
		jsonField("FilteredAttributes"),
		tsField("TimeUnix"),
		f64Field("Value"),
		strField("SpanId"),
		strField("TraceId"),
	))
}

func tracesSchema() *arrow.Schema {
	eventsType := arrow.ListOf(arrow.StructOf(tsField("Timestamp"), strField("Name"), jsonField("Attributes")))
	linksType := arrow.ListOf(arrow.StructOf(strField("TraceId"), strField("SpanId"), strField("TraceState"), jsonField("Attributes")))
	return arrow.NewSchema([]arrow.Field{
		tsField("Timestamp"), strField("TraceId"), strField("SpanId"), strField("ParentSpanId"),
		strField("TraceState"), strField("SpanName"), strField("SpanKind"), strField("ServiceName"),
		jsonField("ResourceAttributes"), strField("ScopeName"), strField("ScopeVersion"),
		jsonField("SpanAttributes"), i64Field("Duration"), strField("StatusCode"), strField("StatusMessage"),
		{Name: "Events", Type: eventsType}, {Name: "Links", Type: linksType},
	}, nil)
}

func logsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		tsField("Timestamp"), strField("TraceId"), strField("SpanId"), i32Field("TraceFlags"),
		strField("SeverityText"), i32Field("SeverityNumber"), strField("ServiceName"), strField("Body"),
		jsonField("ResourceAttributes"), strField("ScopeName"), strField("ScopeVersion"),
		jsonField("ScopeAttributes"), jsonField("LogAttributes"), strField("EventName"),
	}, nil)
}

func metricsCommonFields() []arrow.Field {
	return []arrow.Field{
		jsonField("ResourceAttributes"), strField("ResourceSchemaUrl"),
		strField("ScopeName"), strField("ScopeVersion"), jsonField("ScopeAttributes"), strField("ScopeSchemaUrl"),
		strField("ServiceName"), strField("MetricName"), strField("MetricDescription"), strField("MetricUnit"),
		jsonField("Attributes"), tsField("StartTimeUnix"), tsField("TimeUnix"), i32Field("Flags"),
	}
}

func metricsGaugeSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, f64Field("Value"), arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsSumSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, f64Field("Value"), i32Field("AggregationTemporality"), boolField("IsMonotonic"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsHistogramSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, i64Field("Count"), f64Field("Sum"), i64ListField("BucketCounts"), f64ListField("ExplicitBounds"),
		f64Field("Min"), f64Field("Max"), i32Field("AggregationTemporality"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsExpHistogramSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, i64Field("Count"), f64Field("Sum"), i32Field("Scale"), i64Field("ZeroCount"),
		i32Field("PositiveOffset"), i64ListField("PositiveBucketCounts"),
		i32Field("NegativeOffset"), i64ListField("NegativeBucketCounts"),
		f64Field("Min"), f64Field("Max"), i32Field("AggregationTemporality"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsSummarySchema() *arrow.Schema {
	f := metricsCommonFields()
	quantilesType := arrow.ListOf(arrow.StructOf(f64Field("Quantile"), f64Field("Value")))
	f = append(f, i64Field("Count"), f64Field("Sum"), arrow.Field{Name: "ValueAtQuantiles", Type: quantilesType})
	return arrow.NewSchema(f, nil)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestSchemas -v 2>&1 | tail`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/schema.go exporter/parquetexporter/schema_test.go
git commit -m "feat(parquetexporter): add arrow schemas for all signal tables"
```

---

### Task 6: Traces transform

**Files:**
- Create: `exporter/parquetexporter/traces.go`
- Create: `exporter/parquetexporter/traces_test.go`

**Interfaces:**
- Consumes: `tracesSchema()`, `attributesToJSON`.
- Produces: `func tracesToRecord(td ptrace.Traces) arrow.Record` — one row per span across all resource/scope spans; caller releases the record. Returns a record with zero rows if there are no spans.

- [ ] **Step 1: Write the failing test**

Create `traces_test.go`:

```go
package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTracesToRecord(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.Attributes().PutStr("http.method", "GET")
	ev := span.Events().AppendEmpty()
	ev.SetName("exception")

	rec := tracesToRecord(td)
	defer rec.Release()

	require.Equal(t, int64(1), rec.NumRows())
	svcIdx := rec.Schema().FieldIndices("ServiceName")[0]
	assert.Equal(t, "checkout", rec.Column(svcIdx).(*array.String).Value(0))
	nameIdx := rec.Schema().FieldIndices("SpanName")[0]
	assert.Equal(t, "GET /cart", rec.Column(nameIdx).(*array.String).Value(0))
}

func TestTracesToRecordEmpty(t *testing.T) {
	rec := tracesToRecord(ptrace.NewTraces())
	defer rec.Release()
	assert.Equal(t, int64(0), rec.NumRows())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestTraces 2>&1 | head`
Expected: FAIL — `undefined: tracesToRecord`.

- [ ] **Step 3: Write `traces.go`**

```go
package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func serviceName(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get("service.name"); ok {
		return v.AsString()
	}
	return ""
}

func tracesToRecord(td ptrace.Traces) arrow.Record {
	schema := tracesSchema()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := serviceName(rs.Resource())
		resAttrs := attributesToJSON(rs.Resource().Attributes())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scopeName := ss.Scope().Name()
			scopeVer := ss.Scope().Version()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				appendSpanRow(rb, schema, spans.At(k), svc, resAttrs, scopeName, scopeVer)
			}
		}
	}
	return rb.NewRecord()
}

// appendSpanRow appends exactly one value to every column builder in schema
// order. Every column must be appended once per row or NewRecord panics with a
// length mismatch.
func appendSpanRow(rb *array.RecordBuilder, schema *arrow.Schema, s ptrace.Span, svc, resAttrs, scopeName, scopeVer string) {
	idx := func(name string) int { return schema.FieldIndices(name)[0] }
	str := func(name, v string) { rb.Field(idx(name)).(*array.StringBuilder).Append(v) }
	i64 := func(name string, v int64) { rb.Field(idx(name)).(*array.Int64Builder).Append(v) }

	i64("Timestamp", int64(s.StartTimestamp()))
	str("TraceId", s.TraceID().String())
	str("SpanId", s.SpanID().String())
	str("ParentSpanId", s.ParentSpanID().String())
	str("TraceState", s.TraceState().AsRaw())
	str("SpanName", s.Name())
	str("SpanKind", s.Kind().String())
	str("ServiceName", svc)
	str("ResourceAttributes", resAttrs)
	str("ScopeName", scopeName)
	str("ScopeVersion", scopeVer)
	str("SpanAttributes", attributesToJSON(s.Attributes()))
	i64("Duration", int64(s.EndTimestamp()-s.StartTimestamp()))
	str("StatusCode", s.Status().Code().String())
	str("StatusMessage", s.Status().Message())

	// Events: LIST(STRUCT(Timestamp, Name, Attributes))
	eb := rb.Field(idx("Events")).(*array.ListBuilder)
	eb.Append(true)
	es := eb.ValueBuilder().(*array.StructBuilder)
	for ei := 0; ei < s.Events().Len(); ei++ {
		ev := s.Events().At(ei)
		es.Append(true)
		es.FieldBuilder(0).(*array.Int64Builder).Append(int64(ev.Timestamp()))
		es.FieldBuilder(1).(*array.StringBuilder).Append(ev.Name())
		es.FieldBuilder(2).(*array.StringBuilder).Append(attributesToJSON(ev.Attributes()))
	}

	// Links: LIST(STRUCT(TraceId, SpanId, TraceState, Attributes))
	lb := rb.Field(idx("Links")).(*array.ListBuilder)
	lb.Append(true)
	ls := lb.ValueBuilder().(*array.StructBuilder)
	for li := 0; li < s.Links().Len(); li++ {
		ln := s.Links().At(li)
		ls.Append(true)
		ls.FieldBuilder(0).(*array.StringBuilder).Append(ln.TraceID().String())
		ls.FieldBuilder(1).(*array.StringBuilder).Append(ln.SpanID().String())
		ls.FieldBuilder(2).(*array.StringBuilder).Append(ln.TraceState().AsRaw())
		ls.FieldBuilder(3).(*array.StringBuilder).Append(attributesToJSON(ln.Attributes()))
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestTraces -v 2>&1 | tail`
Expected: PASS. If it panics with "array length mismatch", a column was appended zero or two times — fix the offending column.

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/traces.go exporter/parquetexporter/traces_test.go
git commit -m "feat(parquetexporter): add traces-to-arrow transform"
```

---

### Task 7: Logs transform

**Files:**
- Create: `exporter/parquetexporter/logs.go`
- Create: `exporter/parquetexporter/logs_test.go`

**Interfaces:**
- Consumes: `logsSchema()`, `attributesToJSON`, `serviceName`.
- Produces: `func logsToRecord(ld plog.Logs) arrow.Record` — one row per log record; caller releases. Zero rows if none.

- [ ] **Step 1: Write the failing test**

Create `logs_test.go`:

```go
package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsToRecord(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("boom")
	lr.SetSeverityText("ERROR")

	rec := logsToRecord(ld)
	defer rec.Release()
	require.Equal(t, int64(1), rec.NumRows())
	bodyIdx := rec.Schema().FieldIndices("Body")[0]
	assert.Equal(t, "boom", rec.Column(bodyIdx).(*array.String).Value(0))
}

func TestLogsToRecordEmpty(t *testing.T) {
	rec := logsToRecord(plog.NewLogs())
	defer rec.Release()
	assert.Equal(t, int64(0), rec.NumRows())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestLogs 2>&1 | head`
Expected: FAIL — `undefined: logsToRecord`.

- [ ] **Step 3: Write `logs.go`**

```go
package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"
)

func logsToRecord(ld plog.Logs) arrow.Record {
	schema := logsSchema()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	idx := func(name string) int { return schema.FieldIndices(name)[0] }
	str := func(name, v string) { rb.Field(idx(name)).(*array.StringBuilder).Append(v) }
	i32 := func(name string, v int32) { rb.Field(idx(name)).(*array.Int32Builder).Append(v) }
	i64 := func(name string, v int64) { rb.Field(idx(name)).(*array.Int64Builder).Append(v) }

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		svc := serviceName(rl.Resource())
		resAttrs := attributesToJSON(rl.Resource().Attributes())
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scopeName := sl.Scope().Name()
			scopeVer := sl.Scope().Version()
			scopeAttrs := attributesToJSON(sl.Scope().Attributes())
			recs := sl.LogRecords()
			for k := 0; k < recs.Len(); k++ {
				lr := recs.At(k)
				i64("Timestamp", int64(lr.Timestamp()))
				str("TraceId", lr.TraceID().String())
				str("SpanId", lr.SpanID().String())
				i32("TraceFlags", int32(lr.Flags()))
				str("SeverityText", lr.SeverityText())
				i32("SeverityNumber", int32(lr.SeverityNumber()))
				str("ServiceName", svc)
				str("Body", lr.Body().AsString())
				str("ResourceAttributes", resAttrs)
				str("ScopeName", scopeName)
				str("ScopeVersion", scopeVer)
				str("ScopeAttributes", scopeAttrs)
				str("LogAttributes", attributesToJSON(lr.Attributes()))
				str("EventName", lr.EventName())
			}
		}
	}
	return rb.NewRecord()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestLogs -v 2>&1 | tail`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/logs.go exporter/parquetexporter/logs_test.go
git commit -m "feat(parquetexporter): add logs-to-arrow transform"
```

---

### Task 8: Metrics transform

**Files:**
- Create: `exporter/parquetexporter/metrics.go`
- Create: `exporter/parquetexporter/metrics_test.go`

**Interfaces:**
- Consumes: the five metric schemas, `attributesToJSON`, `serviceName`.
- Produces:
  - `type metricKind int` with consts `kindGauge, kindSum, kindHistogram, kindExpHistogram, kindSummary`.
  - `func metricsToRecords(md pmetric.Metrics) map[metricKind]arrow.Record` — returns a record per kind that has at least one data point; kinds with no data points are absent from the map. Caller releases each record.

- [ ] **Step 1: Write the failing test**

Create `metrics_test.go`:

```go
package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsToRecordsGaugeAndSum(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	g := sm.Metrics().AppendEmpty()
	g.SetName("temp")
	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetDoubleValue(42.0)

	s := sm.Metrics().AppendEmpty()
	s.SetName("reqs")
	sum := s.SetEmptySum()
	sum.SetIsMonotonic(true)
	sp := sum.DataPoints().AppendEmpty()
	sp.SetDoubleValue(7)

	recs := metricsToRecords(md)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	gr, ok := recs[kindGauge]
	require.True(t, ok, "gauge record present")
	require.Equal(t, int64(1), gr.NumRows())
	vIdx := gr.Schema().FieldIndices("Value")[0]
	assert.Equal(t, 42.0, gr.Column(vIdx).(*array.Float64).Value(0))

	sr, ok := recs[kindSum]
	require.True(t, ok, "sum record present")
	require.Equal(t, int64(1), sr.NumRows())
	mIdx := sr.Schema().FieldIndices("IsMonotonic")[0]
	assert.True(t, sr.Column(mIdx).(*array.Boolean).Value(0), "sum IsMonotonic")

	assert.NotContains(t, recs, kindHistogram, "histogram record should be absent")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestMetrics 2>&1 | head`
Expected: FAIL — `undefined: metricsToRecords`.

- [ ] **Step 3: Write `metrics.go`**

```go
package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricKind int

const (
	kindGauge metricKind = iota
	kindSum
	kindHistogram
	kindExpHistogram
	kindSummary
)

type metricMeta struct {
	resAttrs, resSchemaURL              string
	scopeName, scopeVer, scopeAttrs, scopeSchemaURL string
	svc                                 string
	name, desc, unit                    string
}

// builderSet bundles a RecordBuilder with helpers and lazy row count.
type builderSet struct {
	schema *arrow.Schema
	rb     *array.RecordBuilder
	rows   int
}

func newBuilderSet(schema *arrow.Schema) *builderSet {
	return &builderSet{schema: schema, rb: array.NewRecordBuilder(memory.DefaultAllocator, schema)}
}

func (bs *builderSet) idx(name string) int { return bs.schema.FieldIndices(name)[0] }
func (bs *builderSet) str(name, v string)  { bs.rb.Field(bs.idx(name)).(*array.StringBuilder).Append(v) }
func (bs *builderSet) i64(name string, v int64) { bs.rb.Field(bs.idx(name)).(*array.Int64Builder).Append(v) }
func (bs *builderSet) i32(name string, v int32) { bs.rb.Field(bs.idx(name)).(*array.Int32Builder).Append(v) }
func (bs *builderSet) f64(name string, v float64) { bs.rb.Field(bs.idx(name)).(*array.Float64Builder).Append(v) }
func (bs *builderSet) boolean(name string, v bool) { bs.rb.Field(bs.idx(name)).(*array.BooleanBuilder).Append(v) }

func (bs *builderSet) f64List(name string, vals []float64) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Float64Builder)
	for _, v := range vals {
		vb.Append(v)
	}
}

func (bs *builderSet) i64ListFromUint(name string, vals []uint64) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Int64Builder)
	for _, v := range vals {
		vb.Append(int64(v))
	}
}

func (bs *builderSet) common(m metricMeta, attrs pcommon.Map, start, ts pcommon.Timestamp, flags uint32) {
	bs.str("ResourceAttributes", m.resAttrs)
	bs.str("ResourceSchemaUrl", m.resSchemaURL)
	bs.str("ScopeName", m.scopeName)
	bs.str("ScopeVersion", m.scopeVer)
	bs.str("ScopeAttributes", m.scopeAttrs)
	bs.str("ScopeSchemaUrl", m.scopeSchemaURL)
	bs.str("ServiceName", m.svc)
	bs.str("MetricName", m.name)
	bs.str("MetricDescription", m.desc)
	bs.str("MetricUnit", m.unit)
	bs.str("Attributes", attributesToJSON(attrs))
	bs.i64("StartTimeUnix", int64(start))
	bs.i64("TimeUnix", int64(ts))
	bs.i32("Flags", int32(flags))
}

// exemplars appends a LIST(STRUCT(...)) cell from the data point's exemplars.
func (bs *builderSet) exemplars(name string, exs pmetric.ExemplarSlice) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	st := lb.ValueBuilder().(*array.StructBuilder)
	for i := 0; i < exs.Len(); i++ {
		ex := exs.At(i)
		st.Append(true)
		st.FieldBuilder(0).(*array.StringBuilder).Append(attributesToJSON(ex.FilteredAttributes()))
		st.FieldBuilder(1).(*array.Int64Builder).Append(int64(ex.Timestamp()))
		var v float64
		if ex.ValueType() == pmetric.ExemplarValueTypeInt {
			v = float64(ex.IntValue())
		} else {
			v = ex.DoubleValue()
		}
		st.FieldBuilder(2).(*array.Float64Builder).Append(v)
		st.FieldBuilder(3).(*array.StringBuilder).Append(ex.SpanID().String())
		st.FieldBuilder(4).(*array.StringBuilder).Append(ex.TraceID().String())
	}
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

func metricsToRecords(md pmetric.Metrics) map[metricKind]arrow.Record {
	sets := map[metricKind]*builderSet{
		kindGauge:        newBuilderSet(metricsGaugeSchema()),
		kindSum:          newBuilderSet(metricsSumSchema()),
		kindHistogram:    newBuilderSet(metricsHistogramSchema()),
		kindExpHistogram: newBuilderSet(metricsExpHistogramSchema()),
		kindSummary:      newBuilderSet(metricsSummarySchema()),
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				meta := metricMeta{
					resAttrs:       attributesToJSON(res.Attributes()),
					resSchemaURL:   rm.SchemaUrl(),
					scopeName:      scope.Name(),
					scopeVer:       scope.Version(),
					scopeAttrs:     attributesToJSON(scope.Attributes()),
					scopeSchemaURL: sm.SchemaUrl(),
					svc:            serviceName(res),
					name:           m.Name(),
					desc:           m.Description(),
					unit:           m.Unit(),
				}
				appendMetric(sets, m, meta)
			}
		}
	}

	out := map[metricKind]arrow.Record{}
	for kind, bs := range sets {
		if bs.rows > 0 {
			out[kind] = bs.rb.NewRecord()
		}
		bs.rb.Release()
	}
	return out
}

func appendMetric(sets map[metricKind]*builderSet, m pmetric.Metric, meta metricMeta) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		bs := sets[kindGauge]
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.f64("Value", numberValue(dp))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeSum:
		bs := sets[kindSum]
		sum := m.Sum()
		dps := sum.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.f64("Value", numberValue(dp))
			bs.i32("AggregationTemporality", int32(sum.AggregationTemporality()))
			bs.boolean("IsMonotonic", sum.IsMonotonic())
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeHistogram:
		bs := sets[kindHistogram]
		h := m.Histogram()
		dps := h.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			bs.i64ListFromUint("BucketCounts", dp.BucketCounts().AsRaw())
			bs.f64List("ExplicitBounds", dp.ExplicitBounds().AsRaw())
			bs.f64("Min", dp.Min())
			bs.f64("Max", dp.Max())
			bs.i32("AggregationTemporality", int32(h.AggregationTemporality()))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeExponentialHistogram:
		bs := sets[kindExpHistogram]
		eh := m.ExponentialHistogram()
		dps := eh.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			bs.i32("Scale", dp.Scale())
			bs.i64("ZeroCount", int64(dp.ZeroCount()))
			bs.i32("PositiveOffset", dp.Positive().Offset())
			bs.i64ListFromUint("PositiveBucketCounts", dp.Positive().BucketCounts().AsRaw())
			bs.i32("NegativeOffset", dp.Negative().Offset())
			bs.i64ListFromUint("NegativeBucketCounts", dp.Negative().BucketCounts().AsRaw())
			bs.f64("Min", dp.Min())
			bs.f64("Max", dp.Max())
			bs.i32("AggregationTemporality", int32(eh.AggregationTemporality()))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeSummary:
		bs := sets[kindSummary]
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			lb := bs.rb.Field(bs.idx("ValueAtQuantiles")).(*array.ListBuilder)
			lb.Append(true)
			st := lb.ValueBuilder().(*array.StructBuilder)
			qs := dp.QuantileValues()
			for q := 0; q < qs.Len(); q++ {
				qv := qs.At(q)
				st.Append(true)
				st.FieldBuilder(0).(*array.Float64Builder).Append(qv.Quantile())
				st.FieldBuilder(1).(*array.Float64Builder).Append(qv.Value())
			}
			bs.rows++
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestMetrics -v 2>&1 | tail`
Expected: PASS. A builder length-mismatch panic means a column was not appended for some kind — confirm each `case` appends every column in its schema exactly once per data point.

- [ ] **Step 5: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/metrics.go exporter/parquetexporter/metrics_test.go
git commit -m "feat(parquetexporter): add metrics-to-arrow transform (5 tables)"
```

---

### Task 9: Exporter lifecycle and factory

**Files:**
- Create: `exporter/parquetexporter/exporter.go`
- Create: `exporter/parquetexporter/exporter_test.go`
- Create: `exporter/parquetexporter/factory.go`
- Create: `exporter/parquetexporter/factory_test.go`

**Interfaces:**
- Consumes: writers, transforms, `Config`, `newTelemetry` (Task 3).
- Produces:
  - `func newParquetExporter(cfg *Config, set exporter.Settings) (*parquetExporter, error)` — builds the telemetry from `set.TelemetrySettings` (returns its error).
  - methods `Start(ctx, host) error`, `Shutdown(ctx) error`, `pushTraces(ctx, ptrace.Traces) error`, `pushMetrics(ctx, pmetric.Metrics) error`, `pushLogs(ctx, plog.Logs) error`.
  - `func NewFactory() exporter.Factory`.
- Behavior: `Start` creates the seven writers (subdirs `traces`, `logs`, `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exponential_histogram`, `metrics_summary`), passing each its table name and the shared `*telemetry`, then launches a background ticker (`FlushInterval`) that calls `maybeRotateForAge()` on every writer. `Shutdown` stops the ticker and closes every writer. Push methods build the record(s), `write` them, and release.

- [ ] **Step 1: Write the failing test**

Create `exporter_test.go`:

```go
package parquetexporter

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestExporterWritesTraces(t *testing.T) {
	dir := t.TempDir()
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir

	exp, err := newParquetExporter(cfg, exportertest.NewNopSettings(exportertest.NopType))
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))

	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("op")

	require.NoError(t, exp.pushTraces(context.Background(), td))
	require.NoError(t, exp.Shutdown(context.Background()))

	matches, _ := filepath.Glob(filepath.Join(dir, "traces", "*.parquet"))
	assert.Len(t, matches, 1)
}
```

  > NOTE TO IMPLEMENTER: if `exportertest.NewNopSettings(exportertest.NopType)` does not compile against the pinned collector version, use the signature that version exposes (older versions: `exportertest.NewNopSettings()`; check with `go doc go.opentelemetry.io/collector/exporter/exportertest NewNopSettings`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestExporterWrites 2>&1 | head`
Expected: FAIL — `undefined: newParquetExporter`.

- [ ] **Step 3: Write `exporter.go`**

```go
package parquetexporter

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type parquetExporter struct {
	cfg    *Config
	logger *zap.Logger
	tel    *telemetry

	traces  *signalWriter
	logs    *signalWriter
	metrics map[metricKind]*signalWriter

	ticker *time.Ticker
	done   chan struct{}
	wg     sync.WaitGroup
}

func newParquetExporter(cfg *Config, set exporter.Settings) (*parquetExporter, error) {
	tel, err := newTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	return &parquetExporter{cfg: cfg, logger: set.Logger, tel: tel, done: make(chan struct{})}, nil
}

var metricSubdir = map[metricKind]string{
	kindGauge:        "metrics_gauge",
	kindSum:          "metrics_sum",
	kindHistogram:    "metrics_histogram",
	kindExpHistogram: "metrics_exponential_histogram",
	kindSummary:      "metrics_summary",
}

func metricSchemaByKind(kind metricKind) *arrow.Schema {
	switch kind {
	case kindGauge:
		return metricsGaugeSchema()
	case kindSum:
		return metricsSumSchema()
	case kindHistogram:
		return metricsHistogramSchema()
	case kindExpHistogram:
		return metricsExpHistogramSchema()
	default:
		return metricsSummarySchema()
	}
}

func (e *parquetExporter) Start(_ context.Context, _ component.Host) error {
	var err error
	if e.traces, err = newSignalWriter("traces", filepath.Join(e.cfg.Directory, "traces"), tracesSchema(), e.cfg, e.tel, e.logger); err != nil {
		return err
	}
	if e.logs, err = newSignalWriter("logs", filepath.Join(e.cfg.Directory, "logs"), logsSchema(), e.cfg, e.tel, e.logger); err != nil {
		return err
	}
	e.metrics = map[metricKind]*signalWriter{}
	for kind, sub := range metricSubdir {
		// sub is both the subdirectory and the parquet.table attribute value.
		w, werr := newSignalWriter(sub, filepath.Join(e.cfg.Directory, sub), metricSchemaByKind(kind), e.cfg, e.tel, e.logger)
		if werr != nil {
			return werr
		}
		e.metrics[kind] = w
	}

	e.ticker = time.NewTicker(e.cfg.FlushInterval)
	e.wg.Add(1)
	go e.flushLoop()
	return nil
}

func (e *parquetExporter) flushLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.done:
			return
		case <-e.ticker.C:
			e.rotateAllForAge()
		}
	}
}

func (e *parquetExporter) rotateAllForAge() {
	for _, w := range e.allWriters() {
		if err := w.maybeRotateForAge(); err != nil {
			e.logger.Error("parquet age rotation failed", zap.Error(err))
		}
	}
}

func (e *parquetExporter) allWriters() []*signalWriter {
	ws := []*signalWriter{e.traces, e.logs}
	for _, w := range e.metrics {
		ws = append(ws, w)
	}
	return ws
}

func (e *parquetExporter) Shutdown(_ context.Context) error {
	if e.ticker != nil {
		close(e.done)
		e.ticker.Stop()
		e.wg.Wait()
	}
	var firstErr error
	for _, w := range e.allWriters() {
		if w == nil {
			continue
		}
		if err := w.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *parquetExporter) pushTraces(_ context.Context, td ptrace.Traces) error {
	rec := tracesToRecord(td)
	defer rec.Release()
	if rec.NumRows() == 0 {
		return nil
	}
	return e.traces.write(rec)
}

func (e *parquetExporter) pushLogs(_ context.Context, ld plog.Logs) error {
	rec := logsToRecord(ld)
	defer rec.Release()
	if rec.NumRows() == 0 {
		return nil
	}
	return e.logs.write(rec)
}

func (e *parquetExporter) pushMetrics(_ context.Context, md pmetric.Metrics) error {
	recs := metricsToRecords(md)
	for kind, rec := range recs {
		err := e.metrics[kind].write(rec)
		rec.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Write `factory.go`**

```go
package parquetexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const componentType = "parquet"

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(componentType),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelAlpha),
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	exp, err := newParquetExporter(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, exp.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	exp, err := newParquetExporter(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(ctx, set, cfg, exp.pushMetrics,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	exp, err := newParquetExporter(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, exp.pushLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}
```

  > NOTE TO IMPLEMENTER: each signal creator builds its own `parquetExporter`, so three separate instances exist (one per enabled signal) — this matches `natsjetstreamexporter`. Each manages only its own writers; the unused writers on each instance simply stay nil and are skipped by the `w == nil` guard in `Shutdown`.

- [ ] **Step 5: Write `factory_test.go`**

```go
package parquetexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestFactoryCreatesAllSignals(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Directory = t.TempDir()
	set := exportertest.NewNopSettings(exportertest.NopType)

	_, err := f.CreateTraces(context.Background(), set, cfg)
	assert.NoError(t, err)
	_, err = f.CreateMetrics(context.Background(), set, cfg)
	assert.NoError(t, err)
	_, err = f.CreateLogs(context.Background(), set, cfg)
	assert.NoError(t, err)
}
```

  > NOTE TO IMPLEMENTER: method names on the factory (`CreateTraces` vs `CreateTracesExporter`) and `NewNopSettings` signature vary by collector version. Verify with `go doc go.opentelemetry.io/collector/exporter Factory` and adjust.

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./... -v 2>&1 | tail -30`
Expected: PASS (all tests in the package).

- [ ] **Step 7: Commit**

```bash
cd ../.. && make fmt
git add exporter/parquetexporter/exporter.go exporter/parquetexporter/exporter_test.go exporter/parquetexporter/factory.go exporter/parquetexporter/factory_test.go go.mod go.sum
git commit -m "feat(parquetexporter): add exporter lifecycle and factory"
```

---

### Task 10: Wiring, README, and ai-context

**Files:**
- Modify: `Makefile`
- Modify: `distributions/grafts/manifest.yaml`
- Modify: `CLAUDE.md`
- Create: `exporter/parquetexporter/README.md`
- Modify: `distributions/grafts/config.yaml` (optional sample)

**Interfaces:** none (integration only).

- [ ] **Step 1: Add Makefile targets**

In `Makefile`, add the parquet exporter to the `test` and `lint` targets (alongside the existing `@go test -v ./exporter/natsjetstreamexporter/...` lines):

```makefile
	@go test -v ./exporter/parquetexporter/...
```
and
```makefile
	@golangci-lint run ./exporter/parquetexporter/...
```

- [ ] **Step 2: Add the exporter to the OCB manifest**

In `distributions/grafts/manifest.yaml`, under `exporters:`, add (mirroring the natsjetstream entry):

```yaml
  - gomod: go.olly.garden/grafts v0.1.0
    import: go.olly.garden/grafts/exporter/parquetexporter
    path: ../..
```

- [ ] **Step 3: Write `README.md`**

Create `exporter/parquetexporter/README.md`:

```markdown
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
```

- [ ] **Step 4: Update CLAUDE.md (ai-context)**

In `CLAUDE.md`, under `### Components`, add a section after the NATS JetStream Exporter:

```markdown
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
```

  Also add to the `## Configuration` section:

```markdown
**Parquet Exporter** requires:
- `directory`: Root directory for Parquet output (required)
- `flush_interval`: Max age before rotating the open file (default 5m)
- `max_rows`: Max rows before rotating (default 100000)
- `max_bytes`: Max file size before rotating (default 128000000)
- `compression`: Column compression — zstd, snappy, or none (default zstd)
```

- [ ] **Step 5: Verify the full build and tests**

Run:
```bash
make fmt
make test 2>&1 | tail -20
cd distributions/grafts && make build 2>&1 | tail -20
```
Expected: all package tests PASS; OCB build of the distribution succeeds with the parquet exporter included.

  > NOTE TO IMPLEMENTER: if `make build` fails to resolve `go.olly.garden/grafts` versions, ensure the `replaces:` block in `manifest.yaml` still maps `go.olly.garden/grafts => ../..` (it should already, from the natsjetstream setup).

- [ ] **Step 6: Commit**

```bash
cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts
git add Makefile distributions/grafts/manifest.yaml CLAUDE.md exporter/parquetexporter/README.md
git commit -m "feat(parquetexporter): wire into build, docs, and ai-context"
```

---

## Self-Review Notes

- **Spec coverage:** target (arrow-go, no CGo) → Tasks 4-8; local FS only → Task 9 Start; flat per-signal dirs → Task 9 `metricSubdir`/subdir names; JSON attributes → Task 2; rotation time+rows+bytes → Task 4; 5 metric files + nested events/links/exemplars → Tasks 5, 8; name `parquet` → Task 9 factory; ClickHouse-shaped schema → Task 5; Makefile/manifest/CLAUDE.md/README → Task 10; tests incl. round-trip → Tasks 4-9.
- **Telemetry coverage (user request):** instruments + error classification → Task 3; rotation/rows/bytes/error recording + failure logs at each I/O stage → Task 4 writer; per-table wiring + telemetry construction → Task 9 exporter; documented in README + CLAUDE.md → Task 10. Boundaries/signals chosen per the manual-instrumentation skill: metrics + logs for the rotation I/O boundary, no custom spans (deliberate), no duplication of exporterhelper counters.
- **Out-of-scope items** (S3, Hive partitioning, live .duckdb, compaction) intentionally have no tasks.
- **Builder discipline:** every transform must append exactly one value per column per row, or `array.RecordBuilder.NewRecord()` panics with a length mismatch — the per-task tests exercise this.
- **Version pins** (collector `v1.58.0`, arrow-go `v18.6.0`) are starting points; the implementer should align the collector version with the rest of the repo's `go.mod` and adjust `exportertest`/factory API calls per `go doc` if signatures differ.
