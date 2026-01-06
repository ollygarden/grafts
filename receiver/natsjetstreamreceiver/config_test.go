package natsjetstreamreceiver

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
	assert.Equal(t, "otel-receiver", cfg.ConsumerName)
	assert.Equal(t, "natsjetstream-receiver", cfg.ConsumerGroup)
	assert.Equal(t, "otlp.proto.traces", cfg.Subjects.Traces)
	assert.Equal(t, "otlp.proto.metrics", cfg.Subjects.Metrics)
	assert.Equal(t, "otlp.proto.logs", cfg.Subjects.Logs)
	assert.Equal(t, 30*time.Second, cfg.AckWait)
	assert.Equal(t, 5, cfg.MaxDeliver)
	assert.Equal(t, 1000, cfg.MaxAckPending)
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
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "url is required",
		},
		{
			name: "missing stream",
			config: &Config{
				URL:           "nats://localhost:4222",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "stream is required",
		},
		{
			name: "missing consumer name",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "consumer_name is required",
		},
		{
			name: "invalid ack wait",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       0,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "ack_wait must be positive",
		},
		{
			name: "invalid max deliver",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    0,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "max_deliver must be positive",
		},
		{
			name: "invalid max ack pending",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 0,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "max_ack_pending must be positive",
		},
		{
			name: "negative reconnect wait",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				ReconnectWait: -1 * time.Second,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "reconnect_wait must be non-negative",
		},
		{
			name: "invalid ping interval",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				Subjects:      SubjectConfig{Traces: "traces"},
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  0,
			},
			wantErr: "ping_interval must be positive",
		},
		{
			name: "no subjects configured",
			config: &Config{
				URL:           "nats://localhost:4222",
				Stream:        "telemetry",
				ConsumerName:  "test",
				AckWait:       30 * time.Second,
				MaxDeliver:    5,
				MaxAckPending: 1000,
				PingInterval:  2 * time.Minute,
			},
			wantErr: "at least one subject pattern (traces, metrics, logs) must be configured",
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
