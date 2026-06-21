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

// writeOnlyFile wraps *os.File and exposes only Write so that pqarrow.NewFileWriter
// cannot close the underlying file descriptor via its own Close path.
type writeOnlyFile struct{ f *os.File }

func (w writeOnlyFile) Write(p []byte) (int, error) { return w.f.Write(p) }

func newWriterProperties(cfg *Config) (*parquet.WriterProperties, error) {
	codec := compress.Codecs.Zstd
	switch cfg.Compression {
	case compressionSnappy:
		codec = compress.Codecs.Snappy
	case compressionNone:
		codec = compress.Codecs.Uncompressed
	}
	opts := []parquet.WriterProperty{parquet.WithCompression(codec)}
	if cfg.Encryption != nil {
		// Config.Validate already enforces this at startup; we re-decode here and
		// propagate rather than discard, so a writer built from an unvalidated
		// config fails loudly instead of writing with an empty key.
		key, err := cfg.Encryption.decodedKey()
		if err != nil {
			return nil, fmt.Errorf("decode encryption key: %w", err)
		}
		var encOpts []parquet.EncryptOption
		if cfg.Encryption.KeyID != "" {
			encOpts = append(encOpts, parquet.WithFooterKeyMetadata(cfg.Encryption.KeyID))
		}
		fileEnc := parquet.NewFileEncryptionProperties(string(key), encOpts...)
		opts = append(opts, parquet.WithEncryptionProperties(fileEnc))
	}
	return parquet.NewWriterProperties(opts...), nil
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
	props, err := newWriterProperties(cfg)
	if err != nil {
		return nil, fmt.Errorf("build writer properties for %s: %w", table, err)
	}
	return &signalWriter{
		table:  table,
		dir:    dir,
		schema: schema,
		cfg:    cfg,
		props:  props,
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
	fw, err := pqarrow.NewFileWriter(w.schema, writeOnlyFile{f}, w.props, pqarrow.DefaultWriterProps())
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
//
// Order of operations: fw.Close() writes the Parquet footer into the file buffer,
// then file.Sync() fsyncs the footer (and all prior data) to disk, then file.Close()
// releases the fd, and finally os.Rename makes the complete file visible atomically.
// This ensures a hard crash after the rename cannot produce a file with a missing footer.
func (w *signalWriter) rotateLocked(reason string) error {
	if w.fw == nil {
		return nil
	}
	ctx := context.Background()
	start := time.Now()
	rows := w.rows
	partPath := w.partPath

	// fw.Close() writes the Parquet footer. Because we passed a writeOnlyFile wrapper
	// to pqarrow.NewFileWriter, pqarrow cannot close our *os.File — we retain ownership.
	if err := w.fw.Close(); err != nil {
		_ = w.file.Close()
		w.reset()
		w.tel.recordError(ctx, w.table, opWrite, err)
		w.logger.Error("parquet: close writer failed", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("close parquet writer: %w", err)
	}
	// fsync after the footer is written so the complete file is durable before rename.
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		w.reset()
		w.tel.recordError(ctx, w.table, opSync, err)
		w.logger.Error("parquet: fsync failed", zap.String("path", partPath), zap.Error(err))
		return fmt.Errorf("sync: %w", err)
	}
	// Capture size while the fd is still open, then close it.
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

func (w *signalWriter) write(rec arrow.RecordBatch) error {
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
