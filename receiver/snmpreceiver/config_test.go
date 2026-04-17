package snmpreceiver

import (
	"os"
	"testing"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.Equal(t, 60*time.Second, cfg.CollectionInterval)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
	assert.Equal(t, 2, cfg.Retries)
	assert.Equal(t, 25, cfg.MaxRepetitions)
	assert.Nil(t, cfg.TrapListener)
	assert.Empty(t, cfg.Targets)
	assert.Empty(t, cfg.Auth)
	assert.Empty(t, cfg.MetricGroups)
}

func TestValidateRequiresTargetsOrTrapListener(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one target or trap_listener must be configured")
}

func TestValidateTargetRequiresHost(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Port: 161, Auth: "test"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host is required")
}

func TestValidateTargetRequiresAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth is required")
}

func TestValidateTargetRequiresValidAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"existing": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "nonexistent"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
	assert.Contains(t, err.Error(), "not found in auth configs")
}

func TestValidateTargetRequiresValidMetricGroup(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"auth1": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"existing": {
			Metrics: []MetricConfig{
				{OID: "1.3.6.1", MetricName: "test.metric", Type: "gauge"},
			},
		},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "auth1", MetricGroups: []string{"nonexistent_group"}},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_group")
	assert.Contains(t, err.Error(), "not found in metric_groups")
}

func TestValidateAuthV2cRequiresCommunity(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"auth1": {Version: "v2c"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "auth1"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "community is required for v2c")
}

func TestValidateAuthV3RequiresUsername(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"auth1": {Version: "v3"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "auth1"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is required for v3")
}

func TestValidateAuthInvalidVersion(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"auth1": {Version: "v1", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "auth1"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version must be \"v2c\" or \"v3\"")
}

func TestValidateMetricRequiresFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"group1": {
			Metrics: []MetricConfig{
				{MetricName: "test.metric", Type: "gauge"}, // missing OID
			},
		},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "test", MetricGroups: []string{"group1"}},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "oid is required")
}

func TestValidateMetricRequiresType(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"group1": {
			Metrics: []MetricConfig{
				{OID: "1.3.6.1", MetricName: "test.metric"}, // missing Type
			},
		},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "test", MetricGroups: []string{"group1"}},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type is required")
}

func TestValidateMetricInvalidType(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"group1": {
			Metrics: []MetricConfig{
				{OID: "1.3.6.1", MetricName: "test.metric", Type: "histogram"},
			},
		},
	}
	cfg.Targets = []TargetConfig{
		{Host: "10.0.0.1", Auth: "test", MetricGroups: []string{"group1"}},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type must be counter, gauge, or up_down_counter")
}

func TestValidateValidFullConfig(t *testing.T) {
	cfg := &Config{
		CollectionInterval: 30 * time.Second,
		Timeout:            10 * time.Second,
		Retries:            3,
		MaxRepetitions:     50,
		Auth: map[string]AuthConfig{
			"public_v2c": {Version: "v2c", Community: "public"},
			"admin_v3": {
				Version:        "v3",
				Username:       "admin",
				AuthProtocol:   "SHA",
				AuthPassphrase: "authpass",
			},
		},
		Targets: []TargetConfig{
			{
				Host:         "192.168.1.1",
				Port:         161,
				Auth:         "public_v2c",
				MetricGroups: []string{"interface_stats"},
			},
		},
		MetricGroups: map[string]MetricGroupConfig{
			"interface_stats": {
				Walk: "1.3.6.1.2.1.2.2",
				Metrics: []MetricConfig{
					{
						OID:        "1.3.6.1.2.1.2.2.1.10",
						MetricName: "snmp.interface.in_octets",
						Type:       "gauge",
						Unit:       "By",
					},
				},
				Attributes: []AttributeConfig{
					{OID: "1.3.6.1.2.1.2.2.1.2", Name: "interface_name"},
				},
			},
		},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestValidateTrapListenerOnly(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"auth1": {Version: "v2c", Community: "public"},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:162",
		AcceptedAuth:  []string{"auth1"},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestValidateTrapListenerInvalidAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:162",
		AcceptedAuth:  []string{"nonexistent"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
	assert.Contains(t, err.Error(), "not found in auth configs")
}

func TestValidateTrapListenerRequiresListenAddress(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "",
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen_address is required")
}

// loadConfigFromYAML is a helper that reads a YAML file under testdata/
// and decodes only the "receivers.snmp" section into a *Config.
func loadConfigFromYAML(t *testing.T, filename string) *Config {
	t.Helper()

	data, err := os.ReadFile("testdata/" + filename)
	require.NoError(t, err, "reading testdata/%s", filename)

	var raw map[string]any
	err = yaml.Unmarshal(data, &raw)
	require.NoError(t, err, "parsing YAML from testdata/%s", filename)

	receivers, ok := raw["receivers"].(map[string]any)
	require.True(t, ok, "expected 'receivers' key in testdata/%s", filename)

	snmpRaw, ok := receivers["snmp"].(map[string]any)
	require.True(t, ok, "expected 'receivers.snmp' key in testdata/%s", filename)

	// Start from defaults and override with what's in the file.
	cfg := createDefaultConfig().(*Config)
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           cfg,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
		),
	})
	require.NoError(t, err)
	err = decoder.Decode(snmpRaw)
	require.NoError(t, err, "decoding config from testdata/%s", filename)

	return cfg
}

func TestLoadConfigFromYAML(t *testing.T) {
	cfg := loadConfigFromYAML(t, "config.yaml")

	assert.Equal(t, 30*time.Second, cfg.CollectionInterval)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 3, cfg.Retries)
	assert.Equal(t, 50, cfg.MaxRepetitions)

	require.Contains(t, cfg.Auth, "public_v2c")
	auth := cfg.Auth["public_v2c"]
	assert.Equal(t, "v2c", auth.Version)
	assert.Equal(t, "public", auth.Community)

	require.Len(t, cfg.Targets, 1)
	target := cfg.Targets[0]
	assert.Equal(t, "192.168.1.1", target.Host)
	assert.Equal(t, 161, target.Port)
	assert.Equal(t, "public_v2c", target.Auth)
	assert.Equal(t, []string{"interface_stats"}, target.MetricGroups)

	require.Contains(t, cfg.MetricGroups, "interface_stats")
	mg := cfg.MetricGroups["interface_stats"]
	assert.Equal(t, "1.3.6.1.2.1.2.2", mg.Walk)
	require.Len(t, mg.Metrics, 1)
	assert.Equal(t, "1.3.6.1.2.1.2.2.1.10", mg.Metrics[0].OID)
	assert.Equal(t, "snmp.interface.in_octets", mg.Metrics[0].MetricName)
	assert.Equal(t, "gauge", mg.Metrics[0].Type)

	err := cfg.Validate()
	require.NoError(t, err)
}
