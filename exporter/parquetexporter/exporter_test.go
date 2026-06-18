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
