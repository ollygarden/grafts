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
	require.NoError(t, w.close())
}
