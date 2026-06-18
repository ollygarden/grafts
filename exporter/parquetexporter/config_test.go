package parquetexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 5*time.Minute, cfg.FlushInterval)
	assert.Equal(t, int64(100000), cfg.MaxRows)
	assert.Equal(t, int64(128000000), cfg.MaxBytes)
	assert.Equal(t, compressionZstd, cfg.Compression)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) { c.Directory = "/tmp/p" }, false},
		{"missing directory", func(c *Config) { c.Directory = "" }, true},
		{"zero flush interval", func(c *Config) { c.Directory = "/tmp/p"; c.FlushInterval = 0 }, true},
		{"zero max rows", func(c *Config) { c.Directory = "/tmp/p"; c.MaxRows = 0 }, true},
		{"zero max bytes", func(c *Config) { c.Directory = "/tmp/p"; c.MaxBytes = 0 }, true},
		{"bad compression", func(c *Config) { c.Directory = "/tmp/p"; c.Compression = "lz4" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
