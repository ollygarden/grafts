package snmpreceiver

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func allocateUDPPort(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	addr := conn.LocalAddr().String()
	require.NoError(t, conn.Close())
	return addr
}

func TestReceiverStartShutdownTrapOnly(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: allocateUDPPort(t),
		AcceptedAuth:  []string{"test"},
	}

	settings := receivertest.NewNopSettings(factory.Type())
	logsReceiver, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	err = logsReceiver.Start(context.Background(), host)
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

func TestReceiverWarnsOnEmptyAcceptedAuth(t *testing.T) {
	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	factory := NewFactory()
	cfg := createDefaultConfig().(*Config)
	cfg.TrapListener = &TrapListenerConfig{ListenAddress: allocateUDPPort(t)}

	core, observed := observer.New(zapcore.WarnLevel)
	settings := receivertest.NewNopSettings(factory.Type())
	settings.Logger = zap.New(core)
	logsReceiver, err := factory.CreateLogs(context.Background(), settings, cfg, consumertest.NewNop())
	require.NoError(t, err)

	require.NoError(t, logsReceiver.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, logsReceiver.Shutdown(context.Background()))

	assert.Equal(t, 1, observed.FilterMessageSnippet("no accepted_auth").Len())
}
