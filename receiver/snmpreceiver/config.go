package snmpreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the SNMP receiver.
type Config struct {
	CollectionInterval time.Duration                `mapstructure:"collection_interval"`
	Auth               map[string]AuthConfig        `mapstructure:"auth"`
	Targets            []TargetConfig               `mapstructure:"targets"`
	MetricGroups       map[string]MetricGroupConfig `mapstructure:"metric_groups"`
	TrapListener       *TrapListenerConfig          `mapstructure:"trap_listener"`
	Timeout            time.Duration                `mapstructure:"timeout"`
	Retries            int                          `mapstructure:"retries"`
	MaxRepetitions     int                          `mapstructure:"max_repetitions"`
}

// AuthConfig defines authentication settings for SNMP.
type AuthConfig struct {
	Version           string `mapstructure:"version"`
	Community         string `mapstructure:"community"`
	Username          string `mapstructure:"username"`
	AuthProtocol      string `mapstructure:"auth_protocol"`
	AuthPassphrase    string `mapstructure:"auth_passphrase"`
	PrivacyProtocol   string `mapstructure:"privacy_protocol"`
	PrivacyPassphrase string `mapstructure:"privacy_passphrase"`
}

// TargetConfig defines a single SNMP polling target.
type TargetConfig struct {
	Host         string   `mapstructure:"host"`
	Port         int      `mapstructure:"port"`
	Auth         string   `mapstructure:"auth"`
	MetricGroups []string `mapstructure:"metric_groups"`
}

// MetricGroupConfig defines a group of SNMP OIDs to collect together.
type MetricGroupConfig struct {
	Walk             string           `mapstructure:"walk"`
	Metrics          []MetricConfig   `mapstructure:"metrics"`
	Attributes       []AttributeConfig `mapstructure:"attributes"`
	ScalarAttributes []AttributeConfig `mapstructure:"scalar_attributes"`
	Lookups          []LookupConfig   `mapstructure:"lookups"`
}

// MetricConfig defines how to map a single SNMP OID to an OTel metric.
type MetricConfig struct {
	OID         string `mapstructure:"oid"`
	MetricName  string `mapstructure:"metric_name"`
	Type        string `mapstructure:"type"`
	Unit        string `mapstructure:"unit"`
	Description string `mapstructure:"description"`
}

// AttributeConfig defines how to map an SNMP OID to a metric attribute.
type AttributeConfig struct {
	OID  string `mapstructure:"oid"`
	Name string `mapstructure:"name"`
}

// LookupConfig defines an index lookup for enriching metric attributes.
type LookupConfig struct {
	SourceIndexes []string `mapstructure:"source_indexes"`
	LookupOID     string   `mapstructure:"lookup_oid"`
	TargetLabel   string   `mapstructure:"target_label"`
}

// TrapListenerConfig defines settings for receiving SNMP traps.
type TrapListenerConfig struct {
	ListenAddress string   `mapstructure:"listen_address"`
	AcceptedAuth  []string `mapstructure:"accepted_auth"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if len(cfg.Targets) == 0 && cfg.TrapListener == nil {
		return errors.New("at least one target or trap_listener must be configured")
	}

	if len(cfg.Targets) > 0 && cfg.CollectionInterval <= 0 {
		return errors.New("collection_interval must be positive")
	}

	// Validate auth configs
	for name, auth := range cfg.Auth {
		if auth.Version != "v2c" && auth.Version != "v3" {
			return fmt.Errorf("auth %q: version must be \"v2c\" or \"v3\", got %q", name, auth.Version)
		}
		if auth.Version == "v2c" && auth.Community == "" {
			return fmt.Errorf("auth %q: community is required for v2c", name)
		}
		if auth.Version == "v3" && auth.Username == "" {
			return fmt.Errorf("auth %q: username is required for v3", name)
		}
	}

	// Validate targets
	for i, target := range cfg.Targets {
		if target.Host == "" {
			return fmt.Errorf("target[%d]: host is required", i)
		}
		if target.Auth == "" {
			return fmt.Errorf("target[%d]: auth is required", i)
		}
		if _, ok := cfg.Auth[target.Auth]; !ok {
			return fmt.Errorf("target[%d]: auth %q not found in auth configs", i, target.Auth)
		}
		for _, mg := range target.MetricGroups {
			if _, ok := cfg.MetricGroups[mg]; !ok {
				return fmt.Errorf("target[%d]: metric_group %q not found in metric_groups", i, mg)
			}
		}
	}

	// Validate metric groups
	validTypes := map[string]bool{
		"counter":          true,
		"gauge":            true,
		"up_down_counter":  true,
	}
	for name, mg := range cfg.MetricGroups {
		if len(mg.Metrics) == 0 {
			return fmt.Errorf("metric_group %q: at least one metric is required", name)
		}
		for j, metric := range mg.Metrics {
			if metric.OID == "" {
				return fmt.Errorf("metric_group %q metric[%d]: oid is required", name, j)
			}
			if metric.MetricName == "" {
				return fmt.Errorf("metric_group %q metric[%d]: metric_name is required", name, j)
			}
			if metric.Type == "" {
				return fmt.Errorf("metric_group %q metric[%d]: type is required", name, j)
			}
			if !validTypes[metric.Type] {
				return fmt.Errorf("metric_group %q metric[%d]: type must be counter, gauge, or up_down_counter, got %q", name, j, metric.Type)
			}
		}
	}

	// Validate trap listener
	if cfg.TrapListener != nil {
		if cfg.TrapListener.ListenAddress == "" {
			return errors.New("trap_listener: listen_address is required")
		}
		for _, authName := range cfg.TrapListener.AcceptedAuth {
			if _, ok := cfg.Auth[authName]; !ok {
				return fmt.Errorf("trap_listener: accepted_auth %q not found in auth configs", authName)
			}
		}
	}

	return nil
}

// createDefaultConfig creates the default configuration for the receiver.
func createDefaultConfig() component.Config {
	return &Config{
		CollectionInterval: 60 * time.Second,
		Timeout:            5 * time.Second,
		Retries:            2,
		MaxRepetitions:     25,
	}
}
