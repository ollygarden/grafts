package natsjetstreamexporter

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the NATS JetStream exporter.
type Config struct {
	// Connection settings (matches receiver)
	URL             string `mapstructure:"url"`
	CredentialsFile string `mapstructure:"credentials_file"`

	// JetStream settings (matches receiver)
	Domain string `mapstructure:"domain"` // JetStream domain (for clustered deployments)
	Stream string `mapstructure:"stream"` // Stream name (must exist)

	// Subject patterns for each signal type (matches receiver defaults)
	Subjects SubjectConfig `mapstructure:"subjects"`

	// Publishing settings
	PublishAsync bool          `mapstructure:"publish_async"` // Use async publishing for higher throughput
	FlushTimeout time.Duration `mapstructure:"flush_timeout"` // Timeout for flushing pending publishes on shutdown

	// Connection resilience settings (matches receiver)
	ReconnectWait time.Duration `mapstructure:"reconnect_wait"`
	MaxReconnects int           `mapstructure:"max_reconnects"`
	PingInterval  time.Duration `mapstructure:"ping_interval"`
}

// SubjectConfig defines the subject patterns for each signal type.
// Matches the receiver's SubjectConfig exactly for compatibility.
type SubjectConfig struct {
	Traces  string `mapstructure:"traces"`
	Metrics string `mapstructure:"metrics"`
	Logs    string `mapstructure:"logs"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.URL == "" {
		return errors.New("url is required")
	}

	if cfg.Stream == "" {
		return errors.New("stream is required")
	}

	if cfg.FlushTimeout <= 0 {
		return errors.New("flush_timeout must be positive")
	}

	if cfg.ReconnectWait < 0 {
		return errors.New("reconnect_wait must be non-negative")
	}

	if cfg.PingInterval <= 0 {
		return errors.New("ping_interval must be positive")
	}

	if cfg.Subjects.Traces == "" && cfg.Subjects.Metrics == "" && cfg.Subjects.Logs == "" {
		return errors.New("at least one subject pattern (traces, metrics, logs) must be configured")
	}

	return nil
}

// createDefaultConfig creates the default configuration for the exporter.
func createDefaultConfig() component.Config {
	return &Config{
		URL:    "nats://localhost:4222",
		Stream: "telemetry",
		Subjects: SubjectConfig{
			Traces:  "otlp.proto.traces",
			Metrics: "otlp.proto.metrics",
			Logs:    "otlp.proto.logs",
		},
		PublishAsync:  true,
		FlushTimeout:  5 * time.Second,
		ReconnectWait: 2 * time.Second,
		MaxReconnects: -1,
		PingInterval:  2 * time.Minute,
	}
}
