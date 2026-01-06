package natsjetstreamexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.Equal(t, "nats://localhost:4222", cfg.URL)
	assert.Equal(t, "telemetry", cfg.Stream)
	assert.Equal(t, "otlp.proto.traces", cfg.Subjects.Traces)
	assert.Equal(t, "otlp.proto.metrics", cfg.Subjects.Metrics)
	assert.Equal(t, "otlp.proto.logs", cfg.Subjects.Logs)
	assert.True(t, cfg.PublishAsync)
	assert.Equal(t, 5*time.Second, cfg.FlushTimeout)
	assert.Equal(t, 2*time.Second, cfg.ReconnectWait)
	assert.Equal(t, -1, cfg.MaxReconnects)
	assert.Equal(t, 2*time.Minute, cfg.PingInterval)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr string
	}{
		{
			name:    "valid config",
			config:  createDefaultConfig().(*Config),
			wantErr: "",
		},
		{
			name: "missing url",
			config: &Config{
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Traces: "traces"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "url is required",
		},
		{
			name: "missing stream",
			config: &Config{
				URL:          "nats://localhost:4222",
				Subjects:     SubjectConfig{Traces: "traces"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "stream is required",
		},
		{
			name: "invalid flush timeout",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Traces: "traces"},
				FlushTimeout: 0,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "flush_timeout must be positive",
		},
		{
			name: "negative reconnect wait",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				Subjects:      SubjectConfig{Traces: "traces"},
				FlushTimeout:  5 * time.Second,
				ReconnectWait: -1 * time.Second,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "reconnect_wait must be non-negative",
		},
		{
			name: "invalid ping interval",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Traces: "traces"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 0,
			},
			wantErr: "ping_interval must be positive",
		},
		{
			name: "no subjects configured",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "at least one subject pattern (traces, metrics, logs) must be configured",
		},
		{
			name: "only traces subject configured",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Traces: "traces"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "",
		},
		{
			name: "only metrics subject configured",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Metrics: "metrics"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "",
		},
		{
			name: "only logs subject configured",
			config: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "telemetry",
				Subjects:     SubjectConfig{Logs: "logs"},
				FlushTimeout: 5 * time.Second,
				PingInterval: 2 * time.Minute,
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
