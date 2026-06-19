package parquetexporter

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	compressionZstd   = "zstd"
	compressionSnappy = "snappy"
	compressionNone   = "none"
)

// Config defines configuration for the Parquet exporter.
type Config struct {
	// Directory is the root directory under which per-signal subdirectories
	// and Parquet files are written. Required.
	Directory string `mapstructure:"directory"`

	// FlushInterval rotates (closes) the open file once it reaches this age.
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// MaxRows rotates the open file once it holds this many rows.
	MaxRows int64 `mapstructure:"max_rows"`

	// MaxBytes rotates the open file once it reaches this size on disk.
	MaxBytes int64 `mapstructure:"max_bytes"`

	// Compression is the Parquet column compression: zstd, snappy, or none.
	Compression string `mapstructure:"compression"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Directory == "" {
		return errors.New("directory is required")
	}
	if cfg.FlushInterval <= 0 {
		return errors.New("flush_interval must be positive")
	}
	if cfg.MaxRows <= 0 {
		return errors.New("max_rows must be positive")
	}
	if cfg.MaxBytes <= 0 {
		return errors.New("max_bytes must be positive")
	}
	switch cfg.Compression {
	case compressionZstd, compressionSnappy, compressionNone:
	default:
		return errors.New("compression must be one of: zstd, snappy, none")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		FlushInterval: 5 * time.Minute,
		MaxRows:       100000,
		MaxBytes:      128000000,
		Compression:   compressionZstd,
	}
}
