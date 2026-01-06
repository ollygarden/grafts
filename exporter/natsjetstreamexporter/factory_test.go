package natsjetstreamexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)
	assert.Equal(t, componentType, factory.Type().String())
}

func TestFactoryCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, cfg.(*Config).Validate())
}

func TestCreateExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	settings := exportertest.NewNopSettings(factory.Type())

	tests := []struct {
		name       string
		createFunc func() error
	}{
		{
			name: "traces",
			createFunc: func() error {
				e, err := factory.CreateTraces(context.Background(), settings, cfg)
				if err != nil {
					return err
				}
				assert.NotNil(t, e)
				return nil
			},
		},
		{
			name: "metrics",
			createFunc: func() error {
				e, err := factory.CreateMetrics(context.Background(), settings, cfg)
				if err != nil {
					return err
				}
				assert.NotNil(t, e)
				return nil
			},
		},
		{
			name: "logs",
			createFunc: func() error {
				e, err := factory.CreateLogs(context.Background(), settings, cfg)
				if err != nil {
					return err
				}
				assert.NotNil(t, e)
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.createFunc()
			require.NoError(t, err)
		})
	}
}
