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
	set := exportertest.NewNopSettings(f.Type())

	_, err := f.CreateTraces(context.Background(), set, cfg)
	assert.NoError(t, err)
	_, err = f.CreateMetrics(context.Background(), set, cfg)
	assert.NoError(t, err)
	_, err = f.CreateLogs(context.Background(), set, cfg)
	assert.NoError(t, err)
}
