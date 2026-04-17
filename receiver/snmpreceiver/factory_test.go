package snmpreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)
	assert.Equal(t, componentType, factory.Type().String())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	// Default config has no targets or trap_listener, so Validate() will fail — that's expected.
	err := cfg.(*Config).Validate()
	assert.Error(t, err)
}

func TestCreateMetricsReceiver(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := validConfig()
	settings := receivertest.NewNopSettings(factory.Type())

	r, err := factory.CreateMetrics(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestCreateLogsReceiver(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := validConfigWithTrapListener()
	settings := receivertest.NewNopSettings(factory.Type())

	r, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestSharedReceiverInstance(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := validConfigWithTrapListener()
	settings := receivertest.NewNopSettings(factory.Type())

	metricsReceiver, err := factory.CreateMetrics(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	logsReceiver, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, logsReceiver)

	// Verify they share the same underlying receiver instance.
	metricsWrapper := metricsReceiver.(*receiverWrapper)
	logsWrapper := logsReceiver.(*receiverWrapper)

	assert.Same(t, metricsWrapper.shared, logsWrapper.shared)

	// Verify all consumers are registered on the shared receiver.
	shared := metricsWrapper.shared.receiver
	assert.NotNil(t, shared.nextMetrics)
	assert.NotNil(t, shared.nextLogs)
}

// validConfig returns a minimal valid config with one target and no trap listener.
func validConfig() *Config {
	return &Config{
		CollectionInterval: createDefaultConfig().(*Config).CollectionInterval,
		Timeout:            createDefaultConfig().(*Config).Timeout,
		Retries:            createDefaultConfig().(*Config).Retries,
		MaxRepetitions:     createDefaultConfig().(*Config).MaxRepetitions,
		Targets: []TargetConfig{
			{Host: "192.168.1.1", Port: 161},
		},
	}
}

// validConfigWithTrapListener returns a valid config that includes a trap listener.
func validConfigWithTrapListener() *Config {
	cfg := validConfig()
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:1162",
	}
	return cfg
}
