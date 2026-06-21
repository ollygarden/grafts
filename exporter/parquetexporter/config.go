package parquetexporter

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	compressionZstd   = "zstd"
	compressionSnappy = "snappy"
	compressionNone   = "none"
)

// EncryptionConfig enables Parquet Modular Encryption (AES-GCM) for written files.
type EncryptionConfig struct {
	// Key is the base64-encoded raw AES key (16, 24, or 32 bytes -> AES-128/192/256).
	Key string `mapstructure:"key"`
	// KeyID is an optional label written as footer-key metadata so a reader
	// (e.g. DuckDB) can select the matching key by name. Never the key itself.
	KeyID string `mapstructure:"key_id"`
}

// decodedKey returns the raw AES key bytes, validating base64 and length.
func (e *EncryptionConfig) decodedKey() ([]byte, error) {
	if e.Key == "" {
		return nil, errors.New("encryption.key is required when encryption is configured")
	}
	raw, err := base64.StdEncoding.DecodeString(e.Key)
	if err != nil {
		return nil, fmt.Errorf("encryption.key is not valid base64: %w", err)
	}
	switch len(raw) {
	case 16, 24, 32:
		return raw, nil
	default:
		return nil, fmt.Errorf("encryption.key must decode to 16, 24, or 32 bytes, got %d", len(raw))
	}
}

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

	// Encryption, when set, enables Parquet Modular Encryption (AES-GCM).
	Encryption *EncryptionConfig `mapstructure:"encryption"`
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
	if cfg.Encryption != nil {
		if _, err := cfg.Encryption.decodedKey(); err != nil {
			return err
		}
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
