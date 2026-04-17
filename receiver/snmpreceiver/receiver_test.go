package snmpreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestReceiverStartShutdown(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := validConfigWithTrapListener()
	settings := receivertest.NewNopSettings(factory.Type())

	metricsReceiver, err := factory.CreateMetrics(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)

	logsReceiver, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)

	host := componenttest.NewNopHost()

	err = metricsReceiver.Start(context.Background(), host)
	require.NoError(t, err)

	err = logsReceiver.Start(context.Background(), host)
	require.NoError(t, err)

	err = metricsReceiver.Shutdown(context.Background())
	require.NoError(t, err)

	err = logsReceiver.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestReceiverMetricsOnlyNoTrapListener(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := validConfig() // no trap listener
	settings := receivertest.NewNopSettings(factory.Type())

	sink := &consumertest.MetricsSink{}
	metricsReceiver, err := factory.CreateMetrics(context.Background(), settings, cfg, sink)
	require.NoError(t, err)

	wrapper := metricsReceiver.(*receiverWrapper)
	recv := wrapper.shared.receiver

	assert.NotNil(t, recv.nextMetrics)
	assert.Nil(t, recv.nextLogs)
}
