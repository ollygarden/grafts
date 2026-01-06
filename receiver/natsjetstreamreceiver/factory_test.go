package natsjetstreamreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
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

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	settings := receivertest.NewNopSettings(factory.Type())

	tests := []struct {
		name       string
		createFunc func() error
	}{
		{
			name: "traces",
			createFunc: func() error {
				r, err := factory.CreateTraces(context.Background(), settings, cfg, consumertest.NewNop())
				if err != nil {
					return err
				}
				assert.NotNil(t, r)
				return nil
			},
		},
		{
			name: "metrics",
			createFunc: func() error {
				r, err := factory.CreateMetrics(context.Background(), settings, cfg, consumertest.NewNop())
				if err != nil {
					return err
				}
				assert.NotNil(t, r)
				return nil
			},
		},
		{
			name: "logs",
			createFunc: func() error {
				r, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
				if err != nil {
					return err
				}
				assert.NotNil(t, r)
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

func TestSharedReceiverInstance(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	settings := receivertest.NewNopSettings(factory.Type())

	// Create traces receiver
	tracesReceiver, err := factory.CreateTraces(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, tracesReceiver)

	// Create metrics receiver with same config
	metricsReceiver, err := factory.CreateMetrics(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	// Create logs receiver with same config
	logsReceiver, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, logsReceiver)

	// Verify they share the same underlying receiver instance
	tracesWrapper := tracesReceiver.(*receiverWrapper)
	metricsWrapper := metricsReceiver.(*receiverWrapper)
	logsWrapper := logsReceiver.(*receiverWrapper)

	assert.Same(t, tracesWrapper.shared, metricsWrapper.shared)
	assert.Same(t, metricsWrapper.shared, logsWrapper.shared)

	// Verify all consumers are registered
	shared := tracesWrapper.shared.receiver
	assert.NotNil(t, shared.nextTraces)
	assert.NotNil(t, shared.nextMetrics)
	assert.NotNil(t, shared.nextLogs)
}
