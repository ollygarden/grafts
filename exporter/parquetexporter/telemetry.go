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
