package parquetexporter

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestValidateEncryption(t *testing.T) {
	base := func() *Config {
		c := createDefaultConfig().(*Config)
		c.Directory = "/tmp/x"
		return c
	}
	// 32 raw bytes -> AES-256, valid.
	key32 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	key16 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	key24 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 24))
	key20 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 20))

	t.Run("nil block is valid", func(t *testing.T) {
		require.NoError(t, base().Validate())
	})
	t.Run("valid 16/24/32 byte keys", func(t *testing.T) {
		for _, k := range []string{key16, key24, key32} {
			c := base()
			c.Encryption = &EncryptionConfig{Key: k}
			require.NoError(t, c.Validate())
		}
	})
	t.Run("key_id optional and allowed", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: key32, KeyID: "key1"}
		require.NoError(t, c.Validate())
	})
	t.Run("missing key", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{}
		require.Error(t, c.Validate())
	})
	t.Run("invalid base64", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: "not!base64!!"}
		require.Error(t, c.Validate())
	})
	t.Run("wrong key length", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: key20}
		require.Error(t, c.Validate())
	})
}
