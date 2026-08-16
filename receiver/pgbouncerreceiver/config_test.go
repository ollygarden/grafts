package pgbouncerreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestDefaultConfig(t *testing.T) {
	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)

	assert.Equal(t, "localhost:6432", cfg.Endpoint)
	assert.Equal(t, "pgbouncer", cfg.Database)
	assert.Equal(t, time.Minute, cfg.CollectionInterval)

	// Compatibility mode roughly doubles the series count, so it stays off
	// until a user turns it on for a migration.
	assert.Equal(t, []Shape{ShapeOTel}, cfg.Emit)

	// Defaults alone must not be valid: without credentials the receiver would
	// start and silently report nothing.
	assert.Error(t, cfg.Validate())
}

func TestDefaultConfigEnablesEveryMetric(t *testing.T) {
	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)

	// Spot-check both sides of the pooler and one process-wide count. A metric
	// added to the registry and left disabled by default would be invisible.
	assert.True(t, cfg.Metrics.DBClientConnectionCount.Enabled)
	assert.True(t, cfg.Metrics.PgbouncerClientConnectionCount.Enabled)
	assert.True(t, cfg.Metrics.PgbouncerPoolCount.Enabled)
}

func validConfig() *Config {
	cfg, _ := createDefaultConfig().(*Config)
	cfg.Username = "pgbouncer"
	cfg.Password = "secret"
	return cfg
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "valid", mutate: func(*Config) {}},
		{
			name:    "no endpoint",
			mutate:  func(c *Config) { c.Endpoint = "" },
			wantErr: "endpoint is required",
		},
		{
			name:    "no username",
			mutate:  func(c *Config) { c.Username = "" },
			wantErr: "username is required",
		},
		{
			name:    "no database",
			mutate:  func(c *Config) { c.Database = "" },
			wantErr: "database is required",
		},
		{
			name:    "empty emit",
			mutate:  func(c *Config) { c.Emit = nil },
			wantErr: "emit must name at least one",
		},
		{
			name:    "unknown shape",
			mutate:  func(c *Config) { c.Emit = []Shape{"influx"} },
			wantErr: `unknown shape "influx"`,
		},
		{
			name:    "duplicate shape",
			mutate:  func(c *Config) { c.Emit = []Shape{ShapeOTel, ShapeOTel} },
			wantErr: `"otel" listed twice`,
		},
		{
			name:    "negative interval",
			mutate:  func(c *Config) { c.CollectionInterval = -time.Second },
			wantErr: "collection_interval must be positive",
		},
		{
			// A timeout that outlasts the interval means a slow scrape is still
			// running when the next is due, which reads as a hung receiver.
			name:    "timeout at least the interval",
			mutate:  func(c *Config) { c.Timeout = c.CollectionInterval },
			wantErr: "must be shorter than collection_interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfigValidateReportsEveryProblem(t *testing.T) {
	cfg := &Config{}

	err := cfg.Validate()
	require.Error(t, err)

	// One round trip should be enough to fix a configuration.
	for _, want := range []string{"endpoint", "username", "database", "collection_interval", "timeout", "emit"} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestConnStringEscapesCredentials(t *testing.T) {
	cfg := validConfig()
	cfg.Password = "p@ss/w?rd"

	// Formatted into the URL rather than escaped, this would parse as a
	// different host and fail as though the endpoint were wrong.
	conn := cfg.connString()
	assert.NotContains(t, conn, "p@ss")
	assert.Contains(t, conn, "p%40ss%2Fw%3Frd")

	assert.Contains(t, conn, "localhost:6432")
	assert.Contains(t, conn, "sslmode=disable")
}

func TestCreateMetricsReceiver(t *testing.T) {
	cfg := validConfig()

	r, err := createMetricsReceiver(t.Context(), receivertest.NewNopSettings(NewFactory().Type()), cfg, nil)
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestCreateMetricsReceiverRejectsForeignConfig(t *testing.T) {
	_, err := createMetricsReceiver(t.Context(), receivertest.NewNopSettings(NewFactory().Type()), &struct{}{}, nil)
	require.Error(t, err)
}
