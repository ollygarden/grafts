//go:build integration

package snmpreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	// TODO(follow-up): pin to a specific tag or digest once a stable snmpsim release is confirmed.
	snmpsimImage = "tandrup/snmpsim:latest"
	snmpsimPort  = "161/udp"
)

func startSnmpsim(ctx context.Context, t *testing.T) (string, uint16) {
	t.Helper()

	// Skip only when Docker itself is unreachable; real container/image failures must fail the test.
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	defer provider.Close()
	if err := provider.Health(ctx); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}

	req := testcontainers.ContainerRequest{
		Image:        snmpsimImage,
		ExposedPorts: []string{snmpsimPort},
		WaitingFor:   wait.ForLog("Listening at UDP/IPv4 endpoint").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start snmpsim container")
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	mappedHost, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, snmpsimPort)
	require.NoError(t, err)

	portNum := mappedPort.Num()

	// Probe readiness via gosnmp: poll sysUpTime.0 until it responds.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		g := &gosnmp.GoSNMP{
			Target:    mappedHost,
			Port:      portNum,
			Community: "public",
			Version:   gosnmp.Version2c,
			Timeout:   2 * time.Second,
			Retries:   0,
		}
		if err := g.Connect(); err == nil {
			resp, getErr := g.Get([]string{"1.3.6.1.2.1.1.3.0"})
			_ = g.Conn.Close()
			if getErr == nil && len(resp.Variables) > 0 {
				return mappedHost, portNum
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("snmpsim did not become ready within 30s")
	return "", 0
}

// newTestConfig builds a *Config targeting snmpsim at host:port.
// withInterfaces adds the interfaces metric group to the v2c target.
// withV3 adds a second SNMPv3 target polling just the system group.
func newTestConfig(host string, port uint16, withV3, withInterfaces bool) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.CollectionInterval = 5 * time.Second

	cfg.Auth = map[string]AuthConfig{
		"public_v2c": {Version: "v2c", Community: "public"},
	}
	if withV3 {
		cfg.Auth["sim_v3"] = AuthConfig{
			Version:           "v3",
			Username:          "simulator",
			AuthProtocol:      "MD5",
			AuthPassphrase:    "auctoritas",
			PrivacyProtocol:   "DES",
			PrivacyPassphrase: "privatus",
		}
	}

	cfg.MetricGroups = map[string]MetricGroupConfig{
		"system": {
			Metrics: []MetricConfig{
				{
					OID:        "1.3.6.1.2.1.1.3.0",
					MetricName: "snmp.system.uptime",
					Type:       "gauge",
					Unit:       "cs",
				},
			},
			ScalarAttributes: []AttributeConfig{
				{OID: "1.3.6.1.2.1.1.5.0", Name: "sys_name"},
			},
		},
	}
	if withInterfaces {
		cfg.MetricGroups["interfaces"] = MetricGroupConfig{
			Walk: "1.3.6.1.2.1.2.2.1",
			Metrics: []MetricConfig{
				{
					OID:        "1.3.6.1.2.1.2.2.1.10",
					MetricName: "snmp.interface.in_octets",
					Type:       "counter",
					Unit:       "By",
				},
			},
		}
	}

	v2cGroups := []string{"system"}
	if withInterfaces {
		v2cGroups = append(v2cGroups, "interfaces")
	}
	cfg.Targets = []TargetConfig{
		{
			Host:         host,
			Port:         int(port),
			Auth:         "public_v2c",
			MetricGroups: v2cGroups,
		},
	}
	if withV3 {
		cfg.Targets = append(cfg.Targets, TargetConfig{
			Host:         host,
			Port:         int(port),
			Auth:         "sim_v3",
			MetricGroups: []string{"system"},
		})
	}

	return cfg
}

// waitForMetric polls sink.AllMetrics() until a metric with the given name appears,
// or the deadline passes (then t.Fatalf).
func waitForMetric(t *testing.T, sink *consumertest.MetricsSink, metricName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, md := range sink.AllMetrics() {
			for i := 0; i < md.ResourceMetrics().Len(); i++ {
				rm := md.ResourceMetrics().At(i)
				for j := 0; j < rm.ScopeMetrics().Len(); j++ {
					sm := rm.ScopeMetrics().At(j)
					for k := 0; k < sm.Metrics().Len(); k++ {
						if sm.Metrics().At(k).Name() == metricName {
							return
						}
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("metric %q did not appear within %s", metricName, timeout)
}

// findMetric returns the first pmetric.Metric with the given name from the sink, or zero value + false.
func findMetric(sink *consumertest.MetricsSink, name string) (pmetric.Metric, bool) {
	for _, md := range sink.AllMetrics() {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					if m.Name() == name {
						return m, true
					}
				}
			}
		}
	}
	return pmetric.Metric{}, false
}

func TestIntegration_Polling_V2c(t *testing.T) {
	ctx := context.Background()
	host, port := startSnmpsim(ctx, t)

	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	cfg := newTestConfig(host, port, false, true) // no v3, yes interfaces
	sink := new(consumertest.MetricsSink)
	factory := NewFactory()
	settings := receivertest.NewNopSettings(factory.Type())

	r, err := factory.CreateMetrics(ctx, settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	waitForMetric(t, sink, "snmp.system.uptime", 30*time.Second)
	waitForMetric(t, sink, "snmp.interface.in_octets", 30*time.Second)

	// Assert snmp.interface.in_octets has at least one data point.
	m, found := findMetric(sink, "snmp.interface.in_octets")
	require.True(t, found, "snmp.interface.in_octets not found after waiting")

	// counter type maps to Sum in OTel.
	require.Equal(t, pmetric.MetricTypeSum, m.Type(), "expected Sum metric for counter type")
	dps := m.Sum().DataPoints()
	require.Greater(t, dps.Len(), 0, "expected at least one data point for snmp.interface.in_octets")

	// Assert at least one data point carries the interfaces_index attribute (from the table walk index).
	foundIndexed := false
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if _, ok := dp.Attributes().Get("interfaces_index"); ok {
			foundIndexed = true
			break
		}
	}
	assert.True(t, foundIndexed, "expected at least one data point with interfaces_index attribute from the interface table walk")

	// Assert resource carries snmp.host attribute set to the target host.
	foundSnmpHost := false
	for _, md := range sink.AllMetrics() {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			attrs := rm.Resource().Attributes()
			if v, ok := attrs.Get("snmp.host"); ok {
				assert.Equal(t, host, v.Str(), "snmp.host resource attribute should match target host")
				foundSnmpHost = true
			}
		}
	}
	assert.True(t, foundSnmpHost, "expected snmp.host resource attribute on at least one ResourceMetrics")
}

func TestIntegration_Polling_V3(t *testing.T) {
	ctx := context.Background()
	host, port := startSnmpsim(ctx, t)

	t.Cleanup(func() { store.receivers = make(map[component.ID]*sharedReceiver) })

	cfg := newTestConfig(host, port, true, false) // yes v3, no interfaces
	sink := new(consumertest.MetricsSink)
	factory := NewFactory()
	settings := receivertest.NewNopSettings(factory.Type())

	r, err := factory.CreateMetrics(ctx, settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	// Wait up to 45s for snmp.system.uptime to arrive (v3 auth handshake is slower).
	waitForMetric(t, sink, "snmp.system.uptime", 45*time.Second)

	m, found := findMetric(sink, "snmp.system.uptime")
	require.True(t, found)
	assert.Equal(t, pmetric.MetricTypeGauge, m.Type(), "expected Gauge metric for gauge type")
	assert.Greater(t, m.Gauge().DataPoints().Len(), 0, "expected at least one data point for snmp.system.uptime")
}
