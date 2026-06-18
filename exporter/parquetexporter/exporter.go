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
