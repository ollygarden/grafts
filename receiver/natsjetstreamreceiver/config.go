package natsjetstreamreceiver

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the NATS JetStream receiver.
type Config struct {
	// Connection settings
	URL             string `mapstructure:"url"`
	CredentialsFile string `mapstructure:"credentials_file"`

	// JetStream settings
	Domain string `mapstructure:"domain"` // JetStream domain (for clustered deployments)
	Stream string `mapstructure:"stream"`

	// Consumer settings
	ConsumerName  string `mapstructure:"consumer_name"`
	ConsumerGroup string `mapstructure:"consumer_group"`

	// Subject patterns for each signal type
	Subjects SubjectConfig `mapstructure:"subjects"`

	// Reliability settings
	AckWait       time.Duration `mapstructure:"ack_wait"`
	MaxDeliver    int           `mapstructure:"max_deliver"`
	MaxAckPending int           `mapstructure:"max_ack_pending"`

	// Connection resilience settings
	ReconnectWait time.Duration `mapstructure:"reconnect_wait"`
	MaxReconnects int           `mapstructure:"max_reconnects"`
	PingInterval  time.Duration `mapstructure:"ping_interval"`
}

// SubjectConfig defines the subject patterns for each signal type.
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

	if cfg.ConsumerName == "" {
		return errors.New("consumer_name is required")
	}

	if cfg.AckWait <= 0 {
		return errors.New("ack_wait must be positive")
	}

	if cfg.MaxDeliver <= 0 {
		return errors.New("max_deliver must be positive")
	}

	if cfg.MaxAckPending <= 0 {
		return errors.New("max_ack_pending must be positive")
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

// createDefaultConfig creates the default configuration for the receiver.
func createDefaultConfig() component.Config {
	return &Config{
		URL:           "nats://localhost:4222",
		Stream:        "telemetry",
		ConsumerName:  "otel-receiver",
		ConsumerGroup: "natsjetstream-receiver",
		Subjects: SubjectConfig{
			Traces:  "otlp.proto.traces",
			Metrics: "otlp.proto.metrics",
			Logs:    "otlp.proto.logs",
		},
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxAckPending: 1000,
		ReconnectWait: 2 * time.Second,
		MaxReconnects: -1, // Infinite reconnects
		PingInterval:  2 * time.Minute,
	}
}
