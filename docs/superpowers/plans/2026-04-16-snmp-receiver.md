# SNMP Receiver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a unified OpenTelemetry Collector receiver that polls SNMP targets for metrics and listens for SNMP traps as logs.

**Architecture:** Single receiver component (`snmpreceiver`) using gosnmp/gosnmp. Top-level orchestrator wires internal packages: `connection/` (gosnmp wrapper), `poller/` (scheduled SNMP GET/WALK), `trapper/` (UDP trap listener), `metrics/` (pmetric builder), `logs/` (plog builder). Follows the OTLP receiver's `internal/` pattern and the NATS JetStream receiver's shared-receiver/factory pattern from this repo.

**Tech Stack:** Go 1.24, gosnmp/gosnmp, OTel Collector SDK v0.143.0+, pdata (pmetric/plog)

**Spec:** `docs/superpowers/specs/2026-04-16-snmp-receiver-design.md`

**Reference code in this repo:**
- `receiver/natsjetstreamreceiver/` -- factory, config, shared receiver patterns
- `exporter/natsjetstreamexporter/` -- config validation patterns

**Reference code on disk (read-only):**
- `~/Projects/src/github.com/open-telemetry/opentelemetry-collector/receiver/otlpreceiver/` -- internal/ package structure
- `~/Projects/src/github.com/telemetrydrops/ai-context/collector-api/receiver.md` -- receiver API guide

---

## File Map

All paths relative to `receiver/snmpreceiver/`.

| File | Responsibility | Created in |
|---|---|---|
| `doc.go` | Package doc + import path | Task 1 |
| `config.go` | All config structs (Config, AuthConfig, TargetConfig, MetricGroupConfig, etc.) + Validate() | Task 2 |
| `config_test.go` | Config validation tests | Task 2 |
| `testdata/config.yaml` | Valid full config for deserialization tests | Task 2 |
| `testdata/config_defaults.yaml` | Minimal config with defaults | Task 2 |
| `testdata/config_invalid_no_targets.yaml` | Invalid: no targets and no trap_listener | Task 2 |
| `testdata/config_invalid_auth.yaml` | Invalid: bad auth references | Task 2 |
| `factory.go` | NewFactory, createDefaultConfig, createMetrics/createLogs, shared receiver store | Task 3 |
| `factory_test.go` | Factory tests | Task 3 |
| `receiver.go` | Orchestrator: Start/Shutdown, wires poller + trapper | Task 4 |
| `receiver_test.go` | Lifecycle tests (skeleton, then extended in later tasks) | Task 4 |
| `internal/connection/connection.go` | Connection interface + GosnmpWrapper + NewConnection() | Task 5 |
| `internal/connection/connection_test.go` | Connection contract tests | Task 5 |
| `internal/connection/mock.go` | MockConnection for testing | Task 5 |
| `internal/metrics/builder.go` | BuildMetrics() -- SNMP responses to pmetric.Metrics | Task 6 |
| `internal/metrics/builder_test.go` | Builder tests for all metric types, attributes, indexes | Task 6 |
| `internal/poller/collector.go` | Collector -- collects metric groups from one target via Connection | Task 7 |
| `internal/poller/collector_test.go` | Collector tests with MockConnection | Task 7 |
| `internal/poller/poller.go` | Poller -- schedules per-target goroutines, calls Collector | Task 8 |
| `internal/poller/poller_test.go` | Poller lifecycle + integration tests | Task 8 |
| `internal/logs/builder.go` | BuildLog() -- trap PDU to plog.Logs | Task 9 |
| `internal/logs/builder_test.go` | Builder tests for trap types, varbinds, severity | Task 9 |
| `internal/trapper/trapper.go` | Trapper -- UDP listener, auth check, v1 normalization | Task 10 |
| `internal/trapper/trapper_test.go` | Trapper tests | Task 10 |

Files modified in later tasks:

| File | Modification | Task |
|---|---|---|
| `receiver.go` | Wire poller into Start/Shutdown | Task 8 |
| `receiver_test.go` | Add polling integration tests | Task 8 |
| `receiver.go` | Wire trapper into Start/Shutdown | Task 10 |
| `receiver_test.go` | Add trap integration tests | Task 10 |
| `go.mod` (root) | Add gosnmp dependency | Task 5 |
| `Makefile` (root) | Add snmpreceiver to test/lint targets | Task 1 |
| `distributions/grafts/manifest.yaml` | Add snmpreceiver | Task 11 |
| `distributions/grafts/config.yaml` | Add sample snmpreceiver config | Task 11 |

---

## Phase 1: Scaffolding

### Task 1: Package skeleton and build integration

**Files:**
- Create: `receiver/snmpreceiver/doc.go`
- Modify: `Makefile`

- [ ] **Step 1: Create doc.go**

```go
// Package snmpreceiver implements a receiver that polls SNMP targets for
// metrics and listens for SNMP traps/informs as logs.
//
// The receiver supports SNMPv2c and SNMPv3, with configurable metric groups
// that define which OIDs to collect and how to map them to OpenTelemetry
// metrics. SNMP traps are converted to OpenTelemetry log records.
package snmpreceiver // import "go.olly.garden/grafts/receiver/snmpreceiver"
```

- [ ] **Step 2: Update root Makefile to include snmpreceiver**

Add snmpreceiver to the test and lint targets. The updated Makefile:

```makefile
.PHONY: test lint fmt tidy build

# All component packages
PACKAGES := ./receiver/... ./exporter/...

# Run tests for all components
test:
	@echo "Testing receiver/natsjetstreamreceiver..."
	@go test -v ./receiver/natsjetstreamreceiver/...
	@echo "Testing exporter/natsjetstreamexporter..."
	@go test -v ./exporter/natsjetstreamexporter/...
	@echo "Testing receiver/snmpreceiver..."
	@go test -v ./receiver/snmpreceiver/...

# Run linter for all components
lint:
	@echo "Linting receiver/natsjetstreamreceiver..."
	@golangci-lint run ./receiver/natsjetstreamreceiver/...
	@echo "Linting exporter/natsjetstreamexporter..."
	@golangci-lint run ./exporter/natsjetstreamexporter/...
	@echo "Linting receiver/snmpreceiver..."
	@golangci-lint run ./receiver/snmpreceiver/...

# Format all components
fmt:
	@go fmt $(PACKAGES)

# Run go mod tidy
tidy:
	@go mod tidy

# Build the test distribution
build:
	$(MAKE) -C distributions/grafts build
```

- [ ] **Step 3: Verify the package compiles**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go build ./receiver/snmpreceiver/...`
Expected: Clean build (no errors).

- [ ] **Step 4: Commit**

```bash
git add receiver/snmpreceiver/doc.go Makefile
git commit -m "feat(snmpreceiver): add package skeleton and build integration"
```

---

### Task 2: Configuration structs and validation

**Files:**
- Create: `receiver/snmpreceiver/config.go`
- Create: `receiver/snmpreceiver/config_test.go`
- Create: `receiver/snmpreceiver/testdata/config.yaml`
- Create: `receiver/snmpreceiver/testdata/config_defaults.yaml`
- Create: `receiver/snmpreceiver/testdata/config_invalid_no_targets.yaml`
- Create: `receiver/snmpreceiver/testdata/config_invalid_auth.yaml`

- [ ] **Step 1: Write config_test.go with all validation tests**

Tests to write (all should fail initially since config.go doesn't exist yet):

```go
package snmpreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.Equal(t, 60*time.Second, cfg.CollectionInterval)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
	assert.Equal(t, 2, cfg.Retries)
	assert.Equal(t, 25, cfg.MaxRepetitions)
}

func TestValidateRequiresTargetsOrTrapListener(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// No targets, no trap_listener
	err := cfg.Validate()
	assert.ErrorContains(t, err, "at least one target or trap_listener must be configured")
}

func TestValidateTargetRequiresHost(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "", Auth: "test", MetricGroups: []string{"grp"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "target host is required")
}

func TestValidateTargetRequiresValidAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"real": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "nonexistent", MetricGroups: []string{"grp"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "references unknown auth")
}

func TestValidateTargetRequiresValidMetricGroup(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"nonexistent"}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "references unknown metric_group")
}

func TestValidateAuthV2cRequiresCommunity(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"bad": {Version: "v2c"}, // missing community
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "bad", MetricGroups: []string{"grp"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "community is required for v2c")
}

func TestValidateAuthV3RequiresUsername(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"bad": {Version: "v3"}, // missing username
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "bad", MetricGroups: []string{"grp"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "username is required for v3")
}

func TestValidateAuthInvalidVersion(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"bad": {Version: "v1"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "bad", MetricGroups: []string{"grp"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "version must be v2c or v3")
}

func TestValidateMetricRequiresFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "", MetricName: "test", Type: "gauge"}}},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"grp"}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "oid is required")
}

func TestValidateMetricInvalidType(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"grp": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "invalid"}}},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"grp"}},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "type must be counter, gauge, or up_down_counter")
}

func TestValidateValidFullConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"public_v2c": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "public_v2c", MetricGroups: []string{"system"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"system": {
			Metrics: []MetricConfig{
				{OID: "1.3.6.1.2.1.1.3.0", MetricName: "snmp.system.uptime", Type: "gauge", Unit: "cs"},
			},
		},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateTrapListenerOnly(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"public_v2c": {Version: "v2c", Community: "public"},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:162",
		AcceptedAuth:  []string{"public_v2c"},
	}
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateTrapListenerInvalidAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"public_v2c": {Version: "v2c", Community: "public"},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:162",
		AcceptedAuth:  []string{"nonexistent"},
	}
	err := cfg.Validate()
	assert.ErrorContains(t, err, "references unknown auth")
}

func TestLoadConfigFromYAML(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	sub, err := cm.Sub("snmpreceiver")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	require.NoError(t, cfg.Validate())

	assert.Equal(t, 30*time.Second, cfg.CollectionInterval)
	assert.Len(t, cfg.Auth, 1)
	assert.Len(t, cfg.Targets, 1)
	assert.Len(t, cfg.MetricGroups, 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: Compilation errors (Config type doesn't exist).

- [ ] **Step 3: Write config.go**

```go
package snmpreceiver

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the SNMP receiver.
type Config struct {
	// CollectionInterval is the polling interval for all targets.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`

	// Auth defines named authentication configurations.
	Auth map[string]AuthConfig `mapstructure:"auth"`

	// Targets defines the SNMP devices to poll.
	Targets []TargetConfig `mapstructure:"targets"`

	// MetricGroups defines named collections of OIDs to poll.
	MetricGroups map[string]MetricGroupConfig `mapstructure:"metric_groups"`

	// TrapListener configures the SNMP trap/inform listener.
	TrapListener *TrapListenerConfig `mapstructure:"trap_listener"`

	// Timeout is the SNMP request timeout per target.
	Timeout time.Duration `mapstructure:"timeout"`

	// Retries is the number of SNMP request retries.
	Retries int `mapstructure:"retries"`

	// MaxRepetitions is the GETBULK max-repetitions value for v2c/v3 walks.
	MaxRepetitions int `mapstructure:"max_repetitions"`
}

// AuthConfig defines SNMP authentication parameters.
type AuthConfig struct {
	// Version is the SNMP version: "v2c" or "v3".
	Version string `mapstructure:"version"`

	// Community is the community string (v2c only).
	Community string `mapstructure:"community"`

	// Username is the USM username (v3 only).
	Username string `mapstructure:"username"`

	// AuthProtocol is the authentication protocol: "MD5" or "SHA" (v3 only).
	AuthProtocol string `mapstructure:"auth_protocol"`

	// AuthPassphrase is the authentication passphrase (v3 only).
	AuthPassphrase string `mapstructure:"auth_passphrase"`

	// PrivacyProtocol is the privacy protocol: "DES" or "AES" (v3 only).
	PrivacyProtocol string `mapstructure:"privacy_protocol"`

	// PrivacyPassphrase is the privacy passphrase (v3 only).
	PrivacyPassphrase string `mapstructure:"privacy_passphrase"`
}

// TargetConfig defines an SNMP target to poll.
type TargetConfig struct {
	// Host is the target hostname or IP address.
	Host string `mapstructure:"host"`

	// Port is the SNMP port (default: 161).
	Port int `mapstructure:"port"`

	// Auth references a named auth configuration.
	Auth string `mapstructure:"auth"`

	// MetricGroups lists the metric groups to collect from this target.
	MetricGroups []string `mapstructure:"metric_groups"`
}

// MetricGroupConfig defines a named group of OIDs to collect.
type MetricGroupConfig struct {
	// Walk is the OID subtree to WALK for table-based metrics.
	// If empty, metrics are fetched with GET (scalar mode).
	Walk string `mapstructure:"walk"`

	// Metrics defines the metrics to collect.
	Metrics []MetricConfig `mapstructure:"metrics"`

	// Attributes defines OIDs whose values become metric attributes (table mode).
	Attributes []AttributeConfig `mapstructure:"attributes"`

	// ScalarAttributes defines OIDs whose values become resource attributes.
	ScalarAttributes []AttributeConfig `mapstructure:"scalar_attributes"`

	// Lookups define index-to-label resolution chains.
	Lookups []LookupConfig `mapstructure:"lookups"`
}

// MetricConfig defines a single metric to collect.
type MetricConfig struct {
	// OID is the SNMP OID to collect.
	OID string `mapstructure:"oid"`

	// MetricName is the OpenTelemetry metric name.
	MetricName string `mapstructure:"metric_name"`

	// Type is the metric type: "counter", "gauge", or "up_down_counter".
	Type string `mapstructure:"type"`

	// Unit is the metric unit (optional).
	Unit string `mapstructure:"unit"`

	// Description is the metric description (optional).
	Description string `mapstructure:"description"`
}

// AttributeConfig defines an OID whose value becomes an attribute.
type AttributeConfig struct {
	// OID is the SNMP OID to fetch.
	OID string `mapstructure:"oid"`

	// Name is the attribute name.
	Name string `mapstructure:"name"`
}

// LookupConfig defines an index-to-label resolution.
type LookupConfig struct {
	// SourceIndexes names the index fields to resolve.
	SourceIndexes []string `mapstructure:"source_indexes"`

	// LookupOID is the OID table to walk for human-readable values.
	LookupOID string `mapstructure:"lookup_oid"`

	// TargetLabel is the resulting attribute name.
	TargetLabel string `mapstructure:"target_label"`
}

// TrapListenerConfig configures the SNMP trap listener.
type TrapListenerConfig struct {
	// ListenAddress is the UDP address to listen on.
	ListenAddress string `mapstructure:"listen_address"`

	// AcceptedAuth lists auth config names whose credentials are accepted.
	AcceptedAuth []string `mapstructure:"accepted_auth"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if len(cfg.Targets) == 0 && cfg.TrapListener == nil {
		return errors.New("at least one target or trap_listener must be configured")
	}

	// Validate auth configs
	for name, auth := range cfg.Auth {
		if err := auth.validate(name); err != nil {
			return err
		}
	}

	// Validate targets
	for i, target := range cfg.Targets {
		if target.Host == "" {
			return fmt.Errorf("targets[%d]: target host is required", i)
		}
		if _, ok := cfg.Auth[target.Auth]; !ok {
			return fmt.Errorf("targets[%d]: references unknown auth %q", i, target.Auth)
		}
		for _, mg := range target.MetricGroups {
			if _, ok := cfg.MetricGroups[mg]; !ok {
				return fmt.Errorf("targets[%d]: references unknown metric_group %q", i, mg)
			}
		}
	}

	// Validate metric groups
	for name, group := range cfg.MetricGroups {
		for j, m := range group.Metrics {
			if m.OID == "" {
				return fmt.Errorf("metric_groups[%s].metrics[%d]: oid is required", name, j)
			}
			if m.MetricName == "" {
				return fmt.Errorf("metric_groups[%s].metrics[%d]: metric_name is required", name, j)
			}
			if m.Type != "counter" && m.Type != "gauge" && m.Type != "up_down_counter" {
				return fmt.Errorf("metric_groups[%s].metrics[%d]: type must be counter, gauge, or up_down_counter", name, j)
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
				return fmt.Errorf("trap_listener: references unknown auth %q", authName)
			}
		}
	}

	return nil
}

func (a AuthConfig) validate(name string) error {
	switch a.Version {
	case "v2c":
		if a.Community == "" {
			return fmt.Errorf("auth[%s]: community is required for v2c", name)
		}
	case "v3":
		if a.Username == "" {
			return fmt.Errorf("auth[%s]: username is required for v3", name)
		}
	default:
		return fmt.Errorf("auth[%s]: version must be v2c or v3, got %q", name, a.Version)
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
```

- [ ] **Step 4: Create testdata YAML files**

`testdata/config.yaml`:
```yaml
snmpreceiver:
  collection_interval: 30s
  auth:
    public_v2c:
      version: v2c
      community: public
  targets:
    - host: 192.168.1.1
      port: 161
      auth: public_v2c
      metric_groups: [system]
  metric_groups:
    system:
      metrics:
        - oid: "1.3.6.1.2.1.1.3.0"
          metric_name: snmp.system.uptime
          type: gauge
          unit: cs
  timeout: 10s
  retries: 3
  max_repetitions: 50
```

`testdata/config_defaults.yaml`:
```yaml
snmpreceiver:
  auth:
    public_v2c:
      version: v2c
      community: public
  targets:
    - host: 192.168.1.1
      auth: public_v2c
      metric_groups: [system]
  metric_groups:
    system:
      metrics:
        - oid: "1.3.6.1.2.1.1.3.0"
          metric_name: snmp.system.uptime
          type: gauge
```

`testdata/config_invalid_no_targets.yaml`:
```yaml
snmpreceiver:
  collection_interval: 60s
```

`testdata/config_invalid_auth.yaml`:
```yaml
snmpreceiver:
  auth:
    bad:
      version: v1
  targets:
    - host: 192.168.1.1
      auth: bad
      metric_groups: [grp]
  metric_groups:
    grp:
      metrics:
        - oid: "1.2.3"
          metric_name: test
          type: gauge
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add receiver/snmpreceiver/config.go receiver/snmpreceiver/config_test.go receiver/snmpreceiver/testdata/
git commit -m "feat(snmpreceiver): add configuration structs and validation"
```

---

### Task 3: Factory and shared receiver pattern

**Files:**
- Create: `receiver/snmpreceiver/factory.go`
- Create: `receiver/snmpreceiver/factory_test.go`

- [ ] **Step 1: Write factory_test.go**

```go
package snmpreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, component.MustNewType("snmp"), f.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, cfg.(*Config).Validate()) // default config is invalid (no targets) -- we expect error
}

func TestCreateMetricsReceiver(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"sys"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"sys": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}

	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("snmp"))
	recv, err := f.CreateMetrics(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	assert.NotNil(t, recv)
}

func TestCreateLogsReceiver(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:10162",
		AcceptedAuth:  []string{"test"},
	}

	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("snmp"))
	recv, err := f.CreateLogs(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	assert.NotNil(t, recv)
}

func TestSharedReceiverInstance(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"sys"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"sys": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}
	cfg.TrapListener = &TrapListenerConfig{
		ListenAddress: "0.0.0.0:10162",
		AcceptedAuth:  []string{"test"},
	}

	settings := receivertest.NewNopSettings(component.MustNewType("snmp"))

	metricsSink := new(consumertest.MetricsSink)
	metricsRecv, err := f.CreateMetrics(context.Background(), settings, cfg, metricsSink)
	require.NoError(t, err)

	logsSink := new(consumertest.LogsSink)
	logsRecv, err := f.CreateLogs(context.Background(), settings, cfg, logsSink)
	require.NoError(t, err)

	// Both should be wrappers around the same underlying receiver
	mw := metricsRecv.(*receiverWrapper)
	lw := logsRecv.(*receiverWrapper)
	assert.Same(t, mw.shared.receiver, lw.shared.receiver)

	// Clean up the store for other tests
	store.remove(settings.ID)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: Compilation errors (NewFactory, receiverWrapper not defined).

- [ ] **Step 3: Write factory.go**

```go
package snmpreceiver

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const componentType = "snmp"

type receiverStore struct {
	mu        sync.Mutex
	receivers map[component.ID]*sharedReceiver
}

type sharedReceiver struct {
	receiver  *snmpReceiver
	id        component.ID
	startOnce sync.Once
	stopOnce  sync.Once
}

var store = &receiverStore{
	receivers: make(map[component.ID]*sharedReceiver),
}

// NewFactory creates a factory for the SNMP receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(componentType),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha),
	)
}

func (s *receiverStore) getOrCreateReceiver(
	id component.ID,
	cfg *Config,
	settings *receiver.Settings,
) (*sharedReceiver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.receivers[id]; ok {
		return r, nil
	}

	recv, err := newSNMPReceiver(cfg, settings)
	if err != nil {
		return nil, err
	}

	shared := &sharedReceiver{
		receiver: recv,
		id:       id,
	}
	s.receivers[id] = shared
	return shared, nil
}

func (s *receiverStore) remove(id component.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.receivers, id)
}

func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	oCfg := cfg.(*Config)
	shared, err := store.getOrCreateReceiver(settings.ID, oCfg, &settings)
	if err != nil {
		return nil, err
	}
	shared.receiver.registerMetricsConsumer(nextConsumer)
	return &receiverWrapper{shared: shared}, nil
}

func createLogsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	oCfg := cfg.(*Config)
	shared, err := store.getOrCreateReceiver(settings.ID, oCfg, &settings)
	if err != nil {
		return nil, err
	}
	shared.receiver.registerLogsConsumer(nextConsumer)
	return &receiverWrapper{shared: shared}, nil
}

type receiverWrapper struct {
	shared *sharedReceiver
}

func (w *receiverWrapper) Start(ctx context.Context, host component.Host) error {
	var err error
	w.shared.startOnce.Do(func() {
		err = w.shared.receiver.Start(ctx, host)
	})
	return err
}

func (w *receiverWrapper) Shutdown(ctx context.Context) error {
	var err error
	w.shared.stopOnce.Do(func() {
		err = w.shared.receiver.Shutdown(ctx)
		store.remove(w.shared.id)
	})
	return err
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: Compilation errors (snmpReceiver type not defined -- that's Task 4).

- [ ] **Step 5: Continue to Task 4 before committing** (factory depends on receiver.go)

---

### Task 4: Receiver orchestrator skeleton

**Files:**
- Create: `receiver/snmpreceiver/receiver.go`
- Create: `receiver/snmpreceiver/receiver_test.go`

- [ ] **Step 1: Write receiver_test.go (skeleton lifecycle tests)**

```go
package snmpreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestReceiverStartShutdown(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"sys"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"sys": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}

	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("snmp"))
	recv, err := f.CreateMetrics(context.Background(), settings, cfg, sink)
	require.NoError(t, err)

	// Start should not error (skeleton does nothing yet)
	err = recv.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	// Shutdown should not error
	err = recv.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestReceiverMetricsOnlyNoTrapListener(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Auth = map[string]AuthConfig{
		"test": {Version: "v2c", Community: "public"},
	}
	cfg.Targets = []TargetConfig{
		{Host: "192.168.1.1", Auth: "test", MetricGroups: []string{"sys"}},
	}
	cfg.MetricGroups = map[string]MetricGroupConfig{
		"sys": {Metrics: []MetricConfig{{OID: "1.2.3", MetricName: "test", Type: "gauge"}}},
	}

	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("snmp"))
	recv, err := f.CreateMetrics(context.Background(), settings, cfg, sink)
	require.NoError(t, err)

	err = recv.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	// Receiver should have no logs consumer
	wrapper := recv.(*receiverWrapper)
	assert.Nil(t, wrapper.shared.receiver.nextLogs)
	assert.NotNil(t, wrapper.shared.receiver.nextMetrics)

	err = recv.Shutdown(context.Background())
	assert.NoError(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: Compilation errors (snmpReceiver not defined).

- [ ] **Step 3: Write receiver.go**

```go
package snmpreceiver

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type snmpReceiver struct {
	config   *Config
	settings *receiver.Settings

	nextMetrics consumer.Metrics
	nextLogs    consumer.Logs

	obsrecv *receiverhelper.ObsReport

	cancel     context.CancelFunc
	shutdownWG sync.WaitGroup
}

func newSNMPReceiver(cfg *Config, settings *receiver.Settings) (*snmpReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "snmp",
		ReceiverCreateSettings: *settings,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create obsreport: %w", err)
	}

	return &snmpReceiver{
		config:   cfg,
		settings: settings,
		obsrecv:  obsrecv,
	}, nil
}

func (r *snmpReceiver) registerMetricsConsumer(mc consumer.Metrics) {
	r.nextMetrics = mc
}

func (r *snmpReceiver) registerLogsConsumer(lc consumer.Logs) {
	r.nextLogs = lc
}

func (r *snmpReceiver) Start(ctx context.Context, _ component.Host) error {
	_, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if r.nextMetrics != nil {
		r.settings.Logger.Info("SNMP polling configured",
			zap.Int("targets", len(r.config.Targets)),
			zap.Duration("interval", r.config.CollectionInterval))
		// TODO: wire poller in Task 8
	}

	if r.nextLogs != nil && r.config.TrapListener != nil {
		r.settings.Logger.Info("SNMP trap listener configured",
			zap.String("address", r.config.TrapListener.ListenAddress))
		// TODO: wire trapper in Task 10
	}

	return nil
}

func (r *snmpReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	r.shutdownWG.Wait()

	r.settings.Logger.Info("SNMP receiver shutdown complete")
	return nil
}
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: All tests PASS (including factory tests from Task 3).

- [ ] **Step 5: Commit Tasks 3 and 4 together**

```bash
git add receiver/snmpreceiver/factory.go receiver/snmpreceiver/factory_test.go receiver/snmpreceiver/receiver.go receiver/snmpreceiver/receiver_test.go
git commit -m "feat(snmpreceiver): add factory, shared receiver pattern, and orchestrator skeleton"
```

---

### Task 5: Connection interface and gosnmp wrapper

**Files:**
- Create: `receiver/snmpreceiver/internal/connection/connection.go`
- Create: `receiver/snmpreceiver/internal/connection/connection_test.go`
- Create: `receiver/snmpreceiver/internal/connection/mock.go`
- Modify: `go.mod` (add gosnmp dependency)

- [ ] **Step 1: Add gosnmp dependency**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go get github.com/gosnmp/gosnmp@latest`

- [ ] **Step 2: Write connection_test.go**

```go
package connection

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockConnectionGet(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(123456),
		"1.3.6.1.2.1.1.5.0": "my-switch",
	})

	results, err := mock.Get([]string{"1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.5.0"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, uint32(123456), results["1.3.6.1.2.1.1.3.0"])
	assert.Equal(t, "my-switch", results["1.3.6.1.2.1.1.5.0"])
}

func TestMockConnectionGetUnknownOID(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{})

	results, err := mock.Get([]string{"1.2.3.4.5"})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestMockConnectionWalk(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.2.2.1.10.1": uint64(1000),
		"1.3.6.1.2.1.2.2.1.10.2": uint64(2000),
		"1.3.6.1.2.1.2.2.1.16.1": uint64(500),
	})

	results, err := mock.Walk("1.3.6.1.2.1.2.2.1.10")
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, uint64(1000), results["1.3.6.1.2.1.2.2.1.10.1"])
	assert.Equal(t, uint64(2000), results["1.3.6.1.2.1.2.2.1.10.2"])
}

func TestMockConnectionError(t *testing.T) {
	mock := NewMockConnection()
	mock.SetError(assert.AnError)

	_, err := mock.Get([]string{"1.2.3"})
	assert.Error(t, err)
}

func TestNewConnectionParams(t *testing.T) {
	params := Params{
		Host:           "192.168.1.1",
		Port:           161,
		Version:        V2c,
		Community:      "public",
		Timeout:        5 * time.Second,
		Retries:        2,
		MaxRepetitions: 25,
	}

	// We can't actually connect in unit tests, but verify params are valid
	assert.Equal(t, "192.168.1.1", params.Host)
	assert.Equal(t, 161, params.Port)
	assert.Equal(t, V2c, params.Version)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/connection/... -count=1`
Expected: Compilation errors.

- [ ] **Step 4: Write connection.go**

```go
package connection

import (
	"fmt"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
)

// Version represents an SNMP version.
type Version int

const (
	V2c Version = iota
	V3
)

// Params holds connection parameters for creating an SNMP connection.
type Params struct {
	Host              string
	Port              int
	Version           Version
	Community         string
	Username          string
	AuthProtocol      string
	AuthPassphrase    string
	PrivacyProtocol   string
	PrivacyPassphrase string
	Timeout           time.Duration
	Retries           int
	MaxRepetitions    int
}

// Connection is the interface for SNMP operations.
// This abstraction allows mocking in tests.
type Connection interface {
	// Get fetches the values of the specified OIDs.
	// Returns a map of OID -> value for OIDs that were found.
	Get(oids []string) (map[string]interface{}, error)

	// Walk performs an SNMP walk of the given OID subtree.
	// Returns a map of full OID -> value for all OIDs under the prefix.
	Walk(oid string) (map[string]interface{}, error)

	// Close closes the connection.
	Close() error
}

// GosnmpWrapper wraps gosnmp.GoSNMP to implement the Connection interface.
type GosnmpWrapper struct {
	client *gosnmp.GoSNMP
}

// NewConnection creates a new SNMP connection using gosnmp.
func NewConnection(params Params) (Connection, error) {
	client := &gosnmp.GoSNMP{
		Target:         params.Host,
		Port:           uint16(params.Port),
		Timeout:        params.Timeout,
		Retries:        params.Retries,
		MaxRepetitions: uint32(params.MaxRepetitions),
	}

	switch params.Version {
	case V2c:
		client.Version = gosnmp.Version2c
		client.Community = params.Community
	case V3:
		client.Version = gosnmp.Version3
		client.SecurityModel = gosnmp.UserSecurityModel
		client.MsgFlags = gosnmp.AuthPriv
		client.SecurityParameters = &gosnmp.UsmSecurityParameters{
			UserName:                 params.Username,
			AuthenticationProtocol:   mapAuthProtocol(params.AuthProtocol),
			AuthenticationPassphrase: params.AuthPassphrase,
			PrivacyProtocol:          mapPrivacyProtocol(params.PrivacyProtocol),
			PrivacyPassphrase:        params.PrivacyPassphrase,
		}
	default:
		return nil, fmt.Errorf("unsupported SNMP version: %d", params.Version)
	}

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to %s:%d: %w", params.Host, params.Port, err)
	}

	return &GosnmpWrapper{client: client}, nil
}

func (w *GosnmpWrapper) Get(oids []string) (map[string]interface{}, error) {
	result, err := w.client.Get(oids)
	if err != nil {
		return nil, err
	}

	values := make(map[string]interface{}, len(result.Variables))
	for _, v := range result.Variables {
		if v.Type == gosnmp.NoSuchObject || v.Type == gosnmp.NoSuchInstance {
			continue
		}
		values[v.Name] = v.Value
	}
	return values, nil
}

func (w *GosnmpWrapper) Walk(oid string) (map[string]interface{}, error) {
	values := make(map[string]interface{})

	var walkFn func(oid string, walkFunc gosnmp.WalkFunc) error
	if w.client.Version == gosnmp.Version1 {
		walkFn = w.client.Walk
	} else {
		walkFn = w.client.BulkWalk
	}

	err := walkFn(oid, func(pdu gosnmp.SnmpPDU) error {
		if pdu.Type == gosnmp.NoSuchObject || pdu.Type == gosnmp.NoSuchInstance {
			return nil
		}
		values[pdu.Name] = pdu.Value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (w *GosnmpWrapper) Close() error {
	return w.client.Conn.Close()
}

func mapAuthProtocol(protocol string) gosnmp.SnmpV3AuthProtocol {
	switch strings.ToUpper(protocol) {
	case "SHA":
		return gosnmp.SHA
	case "MD5":
		return gosnmp.MD5
	default:
		return gosnmp.NoAuth
	}
}

func mapPrivacyProtocol(protocol string) gosnmp.SnmpV3PrivProtocol {
	switch strings.ToUpper(protocol) {
	case "AES":
		return gosnmp.AES
	case "DES":
		return gosnmp.DES
	default:
		return gosnmp.NoPriv
	}
}
```

- [ ] **Step 5: Write mock.go**

```go
package connection

import (
	"strings"
	"sync"
)

// MockConnection is a test double for the Connection interface.
type MockConnection struct {
	mu     sync.Mutex
	values map[string]interface{}
	err    error
}

// NewMockConnection creates a new MockConnection.
func NewMockConnection() *MockConnection {
	return &MockConnection{
		values: make(map[string]interface{}),
	}
}

// SetValues configures the OID -> value map for the mock.
func (m *MockConnection) SetValues(values map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values = values
}

// SetError configures the mock to return an error on all operations.
func (m *MockConnection) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *MockConnection) Get(oids []string) (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}

	results := make(map[string]interface{})
	for _, oid := range oids {
		if v, ok := m.values[oid]; ok {
			results[oid] = v
		}
	}
	return results, nil
}

func (m *MockConnection) Walk(oid string) (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}

	results := make(map[string]interface{})
	prefix := oid + "."
	for k, v := range m.values {
		if strings.HasPrefix(k, prefix) {
			results[k] = v
		}
	}
	return results, nil
}

func (m *MockConnection) Close() error {
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/connection/... -count=1`
Expected: All tests PASS.

- [ ] **Step 7: Run go mod tidy**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go mod tidy`

- [ ] **Step 8: Commit**

```bash
git add receiver/snmpreceiver/internal/connection/ go.mod go.sum
git commit -m "feat(snmpreceiver): add Connection interface, gosnmp wrapper, and mock"
```

---

## Phase 2: Polling

### Task 6: Metric builder (pmetric construction)

**Files:**
- Create: `receiver/snmpreceiver/internal/metrics/builder.go`
- Create: `receiver/snmpreceiver/internal/metrics/builder_test.go`

- [ ] **Step 1: Write builder_test.go**

```go
package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestBuildGaugeMetric(t *testing.T) {
	now := time.Now()
	result := CollectedMetric{
		MetricName:  "snmp.system.uptime",
		Type:        "gauge",
		Unit:        "cs",
		Description: "System uptime",
		DataPoints: []DataPoint{
			{Value: int64(123456), Attributes: map[string]string{}, Timestamp: now},
		},
	}

	md := BuildMetrics(
		"192.168.1.1", 161,
		map[string]string{"sys_name": "switch01"},
		[]CollectedMetric{result},
	)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	rm := md.ResourceMetrics().At(0)

	// Check resource attributes
	attrs := rm.Resource().Attributes()
	host, ok := attrs.Get("snmp.host")
	require.True(t, ok)
	assert.Equal(t, "192.168.1.1", host.Str())
	port, ok := attrs.Get("snmp.port")
	require.True(t, ok)
	assert.Equal(t, int64(161), port.Int())
	sysName, ok := attrs.Get("sys_name")
	require.True(t, ok)
	assert.Equal(t, "switch01", sysName.Str())

	// Check scope
	require.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)
	assert.Equal(t, "go.olly.garden/grafts/receiver/snmpreceiver", sm.Scope().Name())

	// Check metric
	require.Equal(t, 1, sm.Metrics().Len())
	m := sm.Metrics().At(0)
	assert.Equal(t, "snmp.system.uptime", m.Name())
	assert.Equal(t, "cs", m.Unit())
	assert.Equal(t, "System uptime", m.Description())
	assert.Equal(t, pmetric.MetricTypeGauge, m.Type())

	require.Equal(t, 1, m.Gauge().DataPoints().Len())
	dp := m.Gauge().DataPoints().At(0)
	assert.Equal(t, int64(123456), dp.IntValue())
}

func TestBuildCounterMetric(t *testing.T) {
	now := time.Now()
	result := CollectedMetric{
		MetricName:  "snmp.interface.in_octets",
		Type:        "counter",
		Unit:        "By",
		Description: "Bytes received",
		DataPoints: []DataPoint{
			{Value: uint64(1000), Attributes: map[string]string{"interface_name": "eth0"}, Timestamp: now},
			{Value: uint64(2000), Attributes: map[string]string{"interface_name": "eth1"}, Timestamp: now},
		},
	}

	md := BuildMetrics("192.168.1.1", 161, nil, []CollectedMetric{result})

	rm := md.ResourceMetrics().At(0)
	m := rm.ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, pmetric.MetricTypeSum, m.Type())
	assert.True(t, m.Sum().IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, m.Sum().AggregationTemporality())
	assert.Equal(t, 2, m.Sum().DataPoints().Len())

	dp0 := m.Sum().DataPoints().At(0)
	ifName, ok := dp0.Attributes().Get("interface_name")
	require.True(t, ok)
	assert.Equal(t, "eth0", ifName.Str())
}

func TestBuildUpDownCounterMetric(t *testing.T) {
	now := time.Now()
	result := CollectedMetric{
		MetricName: "snmp.tcp.connections",
		Type:       "up_down_counter",
		DataPoints: []DataPoint{
			{Value: int64(42), Attributes: map[string]string{}, Timestamp: now},
		},
	}

	md := BuildMetrics("192.168.1.1", 161, nil, []CollectedMetric{result})

	m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, pmetric.MetricTypeSum, m.Type())
	assert.False(t, m.Sum().IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, m.Sum().AggregationTemporality())
}

func TestBuildMetricsFloatValue(t *testing.T) {
	now := time.Now()
	result := CollectedMetric{
		MetricName: "snmp.temp",
		Type:       "gauge",
		DataPoints: []DataPoint{
			{Value: float64(36.5), Attributes: map[string]string{}, Timestamp: now},
		},
	}

	md := BuildMetrics("192.168.1.1", 161, nil, []CollectedMetric{result})
	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	assert.InDelta(t, 36.5, dp.DoubleValue(), 0.01)
}

func TestBuildMetricsEmpty(t *testing.T) {
	md := BuildMetrics("192.168.1.1", 161, nil, nil)
	assert.Equal(t, 0, md.ResourceMetrics().Len())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/metrics/... -count=1`
Expected: Compilation errors.

- [ ] **Step 3: Write builder.go**

```go
package metrics

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const scopeName = "go.olly.garden/grafts/receiver/snmpreceiver"

// DataPoint represents a single collected data point.
type DataPoint struct {
	Value      interface{}
	Attributes map[string]string
	Timestamp  time.Time
}

// CollectedMetric represents the collected data for one metric definition.
type CollectedMetric struct {
	MetricName  string
	Type        string
	Unit        string
	Description string
	DataPoints  []DataPoint
}

// BuildMetrics constructs pmetric.Metrics from collected SNMP data.
func BuildMetrics(
	host string,
	port int,
	resourceAttrs map[string]string,
	collected []CollectedMetric,
) pmetric.Metrics {
	md := pmetric.NewMetrics()

	if len(collected) == 0 {
		return md
	}

	rm := md.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("snmp.host", host)
	res.Attributes().PutInt("snmp.port", int64(port))
	for k, v := range resourceAttrs {
		res.Attributes().PutStr(k, v)
	}

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)

	for _, cm := range collected {
		m := sm.Metrics().AppendEmpty()
		m.SetName(cm.MetricName)
		m.SetUnit(cm.Unit)
		m.SetDescription(cm.Description)

		switch cm.Type {
		case "gauge":
			m.SetEmptyGauge()
			for _, dp := range cm.DataPoints {
				pdp := m.Gauge().DataPoints().AppendEmpty()
				setDataPointValue(pdp, dp.Value)
				setDataPointAttributes(pdp, dp.Attributes)
				pdp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
			}
		case "counter":
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, dp := range cm.DataPoints {
				pdp := sum.DataPoints().AppendEmpty()
				setDataPointValue(pdp, dp.Value)
				setDataPointAttributes(pdp, dp.Attributes)
				pdp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
			}
		case "up_down_counter":
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(false)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for _, dp := range cm.DataPoints {
				pdp := sum.DataPoints().AppendEmpty()
				setDataPointValue(pdp, dp.Value)
				setDataPointAttributes(pdp, dp.Attributes)
				pdp.SetTimestamp(pcommon.NewTimestampFromTime(dp.Timestamp))
			}
		}
	}

	return md
}

func setDataPointValue(dp pmetric.NumberDataPoint, value interface{}) {
	switch v := value.(type) {
	case int:
		dp.SetIntValue(int64(v))
	case int64:
		dp.SetIntValue(v)
	case uint:
		dp.SetIntValue(int64(v))
	case uint32:
		dp.SetIntValue(int64(v))
	case uint64:
		dp.SetIntValue(int64(v))
	case float64:
		dp.SetDoubleValue(v)
	case float32:
		dp.SetDoubleValue(float64(v))
	}
}

func setDataPointAttributes(dp pmetric.NumberDataPoint, attrs map[string]string) {
	for k, v := range attrs {
		dp.Attributes().PutStr(k, v)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/metrics/... -count=1`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add receiver/snmpreceiver/internal/metrics/
git commit -m "feat(snmpreceiver): add pmetric builder for SNMP responses"
```

---

### Task 7: Collector (metric group collection from a single target)

**Files:**
- Create: `receiver/snmpreceiver/internal/poller/collector.go`
- Create: `receiver/snmpreceiver/internal/poller/collector_test.go`

- [ ] **Step 1: Write collector_test.go**

```go
package poller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
)

func TestCollectScalarMetricGroup(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(123456),
	})

	group := MetricGroupDef{
		Name: "system",
		Metrics: []MetricDef{
			{OID: "1.3.6.1.2.1.1.3.0", MetricName: "snmp.system.uptime", Type: "gauge", Unit: "cs"},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "snmp.system.uptime", results[0].MetricName)
	assert.Equal(t, "gauge", results[0].Type)
	require.Len(t, results[0].DataPoints, 1)
	assert.Equal(t, uint32(123456), results[0].DataPoints[0].Value)
}

func TestCollectTableMetricGroup(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.2.2.1.10.1": uint64(1000),
		"1.3.6.1.2.1.2.2.1.10.2": uint64(2000),
		"1.3.6.1.2.1.2.2.1.2.1":  "eth0",
		"1.3.6.1.2.1.2.2.1.2.2":  "eth1",
	})

	group := MetricGroupDef{
		Name: "if_traffic",
		Walk: "1.3.6.1.2.1.2.2.1",
		Metrics: []MetricDef{
			{OID: "1.3.6.1.2.1.2.2.1.10", MetricName: "snmp.interface.in_octets", Type: "counter", Unit: "By"},
		},
		Attributes: []AttributeDef{
			{OID: "1.3.6.1.2.1.2.2.1.2", Name: "interface_description"},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].DataPoints, 2)

	// Check that index attributes are present
	dp0 := results[0].DataPoints[0]
	assert.Equal(t, "1", dp0.Attributes["if_traffic_index"])
	assert.Equal(t, "eth0", dp0.Attributes["interface_description"])

	dp1 := results[0].DataPoints[1]
	assert.Equal(t, "2", dp1.Attributes["if_traffic_index"])
	assert.Equal(t, "eth1", dp1.Attributes["interface_description"])
}

func TestCollectWithLookups(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.2.2.1.10.1":  uint64(1000),
		"1.3.6.1.2.1.2.2.1.10.2":  uint64(2000),
		"1.3.6.1.2.1.31.1.1.1.1.1": "eth0",
		"1.3.6.1.2.1.31.1.1.1.1.2": "eth1",
	})

	group := MetricGroupDef{
		Name: "if_traffic",
		Walk: "1.3.6.1.2.1.2.2.1",
		Metrics: []MetricDef{
			{OID: "1.3.6.1.2.1.2.2.1.10", MetricName: "snmp.interface.in_octets", Type: "counter"},
		},
		Lookups: []LookupDef{
			{SourceIndexes: []string{"if_traffic_index"}, LookupOID: "1.3.6.1.2.1.31.1.1.1.1", TargetLabel: "interface_name"},
		},
	}

	results, err := Collect(mock, group)
	require.NoError(t, err)
	require.Len(t, results[0].DataPoints, 2)
	assert.Equal(t, "eth0", results[0].DataPoints[0].Attributes["interface_name"])
	assert.Equal(t, "eth1", results[0].DataPoints[1].Attributes["interface_name"])
}

func TestCollectScalarAttributes(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.5.0": "my-switch",
	})

	group := MetricGroupDef{
		Name: "system",
		ScalarAttributes: []AttributeDef{
			{OID: "1.3.6.1.2.1.1.5.0", Name: "sys_name"},
		},
	}

	attrs, err := CollectScalarAttributes(mock, group)
	require.NoError(t, err)
	assert.Equal(t, "my-switch", attrs["sys_name"])
}

func TestCollectConnectionError(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetError(assert.AnError)

	group := MetricGroupDef{
		Name: "system",
		Metrics: []MetricDef{
			{OID: "1.3.6.1.2.1.1.3.0", MetricName: "test", Type: "gauge"},
		},
	}

	_, err := Collect(mock, group)
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/poller/... -count=1`
Expected: Compilation errors.

- [ ] **Step 3: Write collector.go**

```go
package poller

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/metrics"
)

// MetricGroupDef mirrors the config MetricGroupConfig with resolved values.
type MetricGroupDef struct {
	Name             string
	Walk             string
	Metrics          []MetricDef
	Attributes       []AttributeDef
	ScalarAttributes []AttributeDef
	Lookups          []LookupDef
}

// MetricDef mirrors MetricConfig.
type MetricDef struct {
	OID         string
	MetricName  string
	Type        string
	Unit        string
	Description string
}

// AttributeDef mirrors AttributeConfig.
type AttributeDef struct {
	OID  string
	Name string
}

// LookupDef mirrors LookupConfig.
type LookupDef struct {
	SourceIndexes []string
	LookupOID    string
	TargetLabel  string
}

// Collect collects all metrics for a metric group from a single connection.
func Collect(conn connection.Connection, group MetricGroupDef) ([]metrics.CollectedMetric, error) {
	now := time.Now()

	if group.Walk != "" {
		return collectTable(conn, group, now)
	}
	return collectScalar(conn, group, now)
}

// CollectScalarAttributes fetches scalar attribute OIDs and returns them as a map.
func CollectScalarAttributes(conn connection.Connection, group MetricGroupDef) (map[string]string, error) {
	if len(group.ScalarAttributes) == 0 {
		return nil, nil
	}

	oids := make([]string, len(group.ScalarAttributes))
	for i, attr := range group.ScalarAttributes {
		oids[i] = attr.OID
	}

	values, err := conn.Get(oids)
	if err != nil {
		return nil, fmt.Errorf("failed to GET scalar attributes: %w", err)
	}

	attrs := make(map[string]string, len(group.ScalarAttributes))
	for _, attr := range group.ScalarAttributes {
		if v, ok := values[attr.OID]; ok {
			attrs[attr.Name] = fmt.Sprintf("%v", v)
		}
	}
	return attrs, nil
}

func collectScalar(conn connection.Connection, group MetricGroupDef, now time.Time) ([]metrics.CollectedMetric, error) {
	oids := make([]string, len(group.Metrics))
	for i, m := range group.Metrics {
		oids[i] = m.OID
	}

	values, err := conn.Get(oids)
	if err != nil {
		return nil, fmt.Errorf("failed to GET metrics for group %s: %w", group.Name, err)
	}

	var result []metrics.CollectedMetric
	for _, m := range group.Metrics {
		v, ok := values[m.OID]
		if !ok {
			continue
		}
		result = append(result, metrics.CollectedMetric{
			MetricName:  m.MetricName,
			Type:        m.Type,
			Unit:        m.Unit,
			Description: m.Description,
			DataPoints: []metrics.DataPoint{
				{Value: v, Attributes: map[string]string{}, Timestamp: now},
			},
		})
	}
	return result, nil
}

func collectTable(conn connection.Connection, group MetricGroupDef, now time.Time) ([]metrics.CollectedMetric, error) {
	// Walk all metric OIDs and attribute OIDs
	allWalkData := make(map[string]map[string]interface{}) // oid_prefix -> {full_oid -> value}

	for _, m := range group.Metrics {
		walked, err := conn.Walk(m.OID)
		if err != nil {
			return nil, fmt.Errorf("failed to WALK %s in group %s: %w", m.OID, group.Name, err)
		}
		allWalkData[m.OID] = walked
	}

	for _, attr := range group.Attributes {
		walked, err := conn.Walk(attr.OID)
		if err != nil {
			return nil, fmt.Errorf("failed to WALK attribute %s in group %s: %w", attr.OID, group.Name, err)
		}
		allWalkData[attr.OID] = walked
	}

	// Walk lookup OIDs
	lookupData := make(map[string]map[string]interface{})
	for _, lookup := range group.Lookups {
		walked, err := conn.Walk(lookup.LookupOID)
		if err != nil {
			return nil, fmt.Errorf("failed to WALK lookup %s in group %s: %w", lookup.LookupOID, group.Name, err)
		}
		lookupData[lookup.LookupOID] = walked
	}

	// Extract indexes from the first metric's walk results
	var indexes []string
	if len(group.Metrics) > 0 {
		firstMetricOID := group.Metrics[0].OID
		if walkResults, ok := allWalkData[firstMetricOID]; ok {
			indexSet := make(map[string]struct{})
			for fullOID := range walkResults {
				idx := extractIndex(firstMetricOID, fullOID)
				if idx != "" {
					indexSet[idx] = struct{}{}
				}
			}
			for idx := range indexSet {
				indexes = append(indexes, idx)
			}
			sort.Strings(indexes)
		}
	}

	// Build lookup maps: index -> label value
	lookupMaps := make(map[string]map[string]string) // target_label -> {index -> value}
	for _, lookup := range group.Lookups {
		labelMap := make(map[string]string)
		if walked, ok := lookupData[lookup.LookupOID]; ok {
			for fullOID, val := range walked {
				idx := extractIndex(lookup.LookupOID, fullOID)
				if idx != "" {
					labelMap[idx] = fmt.Sprintf("%v", val)
				}
			}
		}
		lookupMaps[lookup.TargetLabel] = labelMap
	}

	// Build collected metrics
	var result []metrics.CollectedMetric
	indexAttrName := group.Name + "_index"

	for _, m := range group.Metrics {
		walked := allWalkData[m.OID]
		var dataPoints []metrics.DataPoint

		for _, idx := range indexes {
			fullOID := m.OID + "." + idx
			v, ok := walked[fullOID]
			if !ok {
				continue
			}

			attrs := map[string]string{
				indexAttrName: idx,
			}

			// Add table attributes
			for _, attr := range group.Attributes {
				attrFullOID := attr.OID + "." + idx
				if attrVal, ok := allWalkData[attr.OID][attrFullOID]; ok {
					attrs[attr.Name] = fmt.Sprintf("%v", attrVal)
				}
			}

			// Add lookup labels
			for label, labelMap := range lookupMaps {
				if labelVal, ok := labelMap[idx]; ok {
					attrs[label] = labelVal
				}
			}

			dataPoints = append(dataPoints, metrics.DataPoint{
				Value:      v,
				Attributes: attrs,
				Timestamp:  now,
			})
		}

		result = append(result, metrics.CollectedMetric{
			MetricName:  m.MetricName,
			Type:        m.Type,
			Unit:        m.Unit,
			Description: m.Description,
			DataPoints:  dataPoints,
		})
	}

	return result, nil
}

// extractIndex extracts the index suffix from a full OID given its prefix.
// e.g., extractIndex("1.3.6.1.2.1.2.2.1.10", "1.3.6.1.2.1.2.2.1.10.1") returns "1"
func extractIndex(prefix, fullOID string) string {
	if !strings.HasPrefix(fullOID, prefix+".") {
		return ""
	}
	return fullOID[len(prefix)+1:]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/poller/... -count=1`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add receiver/snmpreceiver/internal/poller/collector.go receiver/snmpreceiver/internal/poller/collector_test.go
git commit -m "feat(snmpreceiver): add metric group collector with table walks and lookups"
```

---

### Task 8: Poller scheduler and receiver wiring

**Files:**
- Create: `receiver/snmpreceiver/internal/poller/poller.go`
- Create: `receiver/snmpreceiver/internal/poller/poller_test.go`
- Modify: `receiver/snmpreceiver/receiver.go`
- Modify: `receiver/snmpreceiver/receiver_test.go`

- [ ] **Step 1: Write poller_test.go**

```go
package poller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func TestPollerStartStop(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetValues(map[string]interface{}{
		"1.3.6.1.2.1.1.3.0": uint32(100),
	})

	target := TargetDef{
		Host: "192.168.1.1",
		Port: 161,
		Conn: mock,
		MetricGroups: []MetricGroupDef{
			{
				Name: "system",
				Metrics: []MetricDef{
					{OID: "1.3.6.1.2.1.1.3.0", MetricName: "snmp.system.uptime", Type: "gauge"},
				},
			},
		},
	}

	sink := new(consumertest.MetricsSink)
	logger := zaptest.NewLogger(t)
	p := New(logger, []TargetDef{target}, 100*time.Millisecond, sink)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	// Wait for at least one poll cycle
	require.Eventually(t, func() bool {
		return len(sink.AllMetrics()) >= 1
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()

	// Verify we got metrics
	allMetrics := sink.AllMetrics()
	assert.GreaterOrEqual(t, len(allMetrics), 1)

	md := allMetrics[0]
	assert.Equal(t, 1, md.ResourceMetrics().Len())
}

func TestPollerTargetError(t *testing.T) {
	mock := connection.NewMockConnection()
	mock.SetError(assert.AnError)

	target := TargetDef{
		Host: "192.168.1.1",
		Port: 161,
		Conn: mock,
		MetricGroups: []MetricGroupDef{
			{
				Name: "system",
				Metrics: []MetricDef{
					{OID: "1.3.6.1.2.1.1.3.0", MetricName: "test", Type: "gauge"},
				},
			},
		},
	}

	sink := new(consumertest.MetricsSink)
	logger := zaptest.NewLogger(t)
	p := New(logger, []TargetDef{target}, 100*time.Millisecond, sink)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	// Give it time to try a few cycles
	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()

	// Errors should not produce metrics
	assert.Empty(t, sink.AllMetrics())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/poller/... -run TestPoller -count=1`
Expected: Compilation errors (New, TargetDef, Run not defined).

- [ ] **Step 3: Write poller.go**

```go
package poller

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/metrics"
)

// TargetDef defines a target with its connection and metric groups.
type TargetDef struct {
	Host             string
	Port             int
	Conn             connection.Connection
	MetricGroups     []MetricGroupDef
	ResourceAttrs    map[string]string
}

// Poller runs periodic SNMP collection for a set of targets.
type Poller struct {
	logger   *zap.Logger
	targets  []TargetDef
	interval time.Duration
	consumer consumer.Metrics
}

// New creates a new Poller.
func New(
	logger *zap.Logger,
	targets []TargetDef,
	interval time.Duration,
	consumer consumer.Metrics,
) *Poller {
	return &Poller{
		logger:   logger,
		targets:  targets,
		interval: interval,
		consumer: consumer,
	}
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := range p.targets {
		wg.Add(1)
		go func(target *TargetDef) {
			defer wg.Done()
			p.pollTarget(ctx, target)
		}(&p.targets[i])
	}

	wg.Wait()
}

func (p *Poller) pollTarget(ctx context.Context, target *TargetDef) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Do an immediate first poll
	p.collectAndSend(ctx, target)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.collectAndSend(ctx, target)
		}
	}
}

func (p *Poller) collectAndSend(ctx context.Context, target *TargetDef) {
	var allCollected []metrics.CollectedMetric

	for _, group := range target.MetricGroups {
		collected, err := Collect(target.Conn, group)
		if err != nil {
			p.logger.Warn("Failed to collect metric group",
				zap.String("target", target.Host),
				zap.String("group", group.Name),
				zap.Error(err))
			continue
		}
		allCollected = append(allCollected, collected...)
	}

	if len(allCollected) == 0 {
		return
	}

	md := metrics.BuildMetrics(target.Host, target.Port, target.ResourceAttrs, allCollected)
	if err := p.consumer.ConsumeMetrics(ctx, md); err != nil {
		p.logger.Warn("Failed to send metrics to consumer",
			zap.String("target", target.Host),
			zap.Error(err))
	}
}
```

- [ ] **Step 4: Run poller tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/poller/... -count=1`
Expected: All tests PASS.

- [ ] **Step 5: Update receiver.go to wire the poller**

Replace the `Start` and `Shutdown` methods and add helper in `receiver.go`:

In `Start`, replace the `if r.nextMetrics != nil` block:

```go
func (r *snmpReceiver) Start(ctx context.Context, _ component.Host) error {
	loopCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if r.nextMetrics != nil && len(r.config.Targets) > 0 {
		targets, err := r.buildTargetDefs(ctx)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to build target definitions: %w", err)
		}

		p := poller.New(r.settings.Logger, targets, r.config.CollectionInterval, r.nextMetrics)
		r.shutdownWG.Add(1)
		go func() {
			defer r.shutdownWG.Done()
			p.Run(loopCtx)
		}()

		r.settings.Logger.Info("SNMP polling started",
			zap.Int("targets", len(targets)),
			zap.Duration("interval", r.config.CollectionInterval))
	}

	if r.nextLogs != nil && r.config.TrapListener != nil {
		r.settings.Logger.Info("SNMP trap listener configured",
			zap.String("address", r.config.TrapListener.ListenAddress))
		// TODO: wire trapper in Task 10
	}

	return nil
}
```

Add the `buildTargetDefs` helper and the import for `poller` package:

```go
func (r *snmpReceiver) buildTargetDefs(ctx context.Context) ([]poller.TargetDef, error) {
	var targets []poller.TargetDef

	for _, tc := range r.config.Targets {
		auth := r.config.Auth[tc.Auth]
		port := tc.Port
		if port == 0 {
			port = 161
		}

		version := connection.V2c
		if auth.Version == "v3" {
			version = connection.V3
		}

		conn, err := connection.NewConnection(connection.Params{
			Host:              tc.Host,
			Port:              port,
			Version:           version,
			Community:         auth.Community,
			Username:          auth.Username,
			AuthProtocol:      auth.AuthProtocol,
			AuthPassphrase:    auth.AuthPassphrase,
			PrivacyProtocol:   auth.PrivacyProtocol,
			PrivacyPassphrase: auth.PrivacyPassphrase,
			Timeout:           r.config.Timeout,
			Retries:           r.config.Retries,
			MaxRepetitions:    r.config.MaxRepetitions,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s:%d: %w", tc.Host, port, err)
		}

		var groups []poller.MetricGroupDef
		for _, mgName := range tc.MetricGroups {
			mg := r.config.MetricGroups[mgName]
			groups = append(groups, convertMetricGroup(mgName, mg))
		}

		// Collect scalar attributes
		resourceAttrs := map[string]string{}
		for _, group := range groups {
			attrs, err := poller.CollectScalarAttributes(conn, group)
			if err != nil {
				r.settings.Logger.Warn("Failed to collect scalar attributes",
					zap.String("target", tc.Host),
					zap.Error(err))
			}
			for k, v := range attrs {
				resourceAttrs[k] = v
			}
		}

		targets = append(targets, poller.TargetDef{
			Host:          tc.Host,
			Port:          port,
			Conn:          conn,
			MetricGroups:  groups,
			ResourceAttrs: resourceAttrs,
		})
	}

	return targets, nil
}

func convertMetricGroup(name string, mg MetricGroupConfig) poller.MetricGroupDef {
	var metricDefs []poller.MetricDef
	for _, m := range mg.Metrics {
		metricDefs = append(metricDefs, poller.MetricDef{
			OID: m.OID, MetricName: m.MetricName, Type: m.Type, Unit: m.Unit, Description: m.Description,
		})
	}

	var attrDefs []poller.AttributeDef
	for _, a := range mg.Attributes {
		attrDefs = append(attrDefs, poller.AttributeDef{OID: a.OID, Name: a.Name})
	}

	var scalarAttrDefs []poller.AttributeDef
	for _, a := range mg.ScalarAttributes {
		scalarAttrDefs = append(scalarAttrDefs, poller.AttributeDef{OID: a.OID, Name: a.Name})
	}

	var lookupDefs []poller.LookupDef
	for _, l := range mg.Lookups {
		lookupDefs = append(lookupDefs, poller.LookupDef{
			SourceIndexes: l.SourceIndexes, LookupOID: l.LookupOID, TargetLabel: l.TargetLabel,
		})
	}

	return poller.MetricGroupDef{
		Name: name, Walk: mg.Walk, Metrics: metricDefs, Attributes: attrDefs,
		ScalarAttributes: scalarAttrDefs, Lookups: lookupDefs,
	}
}
```

Add imports to receiver.go: `"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"` and `"go.olly.garden/grafts/receiver/snmpreceiver/internal/poller"`.

Also add a `connections` field to `snmpReceiver` for cleanup:

```go
type snmpReceiver struct {
	// ... existing fields ...
	connections []connection.Connection
}
```

Update `buildTargetDefs` to track connections, and `Shutdown` to close them:

```go
func (r *snmpReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.shutdownWG.Wait()

	for _, conn := range r.connections {
		conn.Close()
	}

	r.settings.Logger.Info("SNMP receiver shutdown complete")
	return nil
}
```

- [ ] **Step 6: Run all tests**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add receiver/snmpreceiver/internal/poller/poller.go receiver/snmpreceiver/internal/poller/poller_test.go receiver/snmpreceiver/receiver.go receiver/snmpreceiver/receiver_test.go
git commit -m "feat(snmpreceiver): add poller scheduler and wire into receiver lifecycle"
```

---

## Phase 3: Trap Receiver

### Task 9: Log builder (plog construction from traps)

**Files:**
- Create: `receiver/snmpreceiver/internal/logs/builder.go`
- Create: `receiver/snmpreceiver/internal/logs/builder_test.go`

- [ ] **Step 1: Write builder_test.go**

```go
package logs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestBuildTrapLogLinkDown(t *testing.T) {
	now := time.Now()
	trap := TrapData{
		SourceIP:   "192.168.1.100",
		SourcePort: 45231,
		Version:    "v2c",
		Community:  "public",
		TrapOID:    "1.3.6.1.6.3.1.1.5.3", // linkDown
		Uptime:     123456,
		Varbinds: map[string]interface{}{
			"1.3.6.1.2.1.2.2.1.1": "2",
			"1.3.6.1.2.1.2.2.1.7": "2",
		},
		Timestamp: now,
	}

	ld := BuildLog(trap)

	require.Equal(t, 1, ld.ResourceLogs().Len())
	rl := ld.ResourceLogs().At(0)

	// Check resource
	host, ok := rl.Resource().Attributes().Get("snmp.host")
	require.True(t, ok)
	assert.Equal(t, "192.168.1.100", host.Str())

	port, ok := rl.Resource().Attributes().Get("snmp.port")
	require.True(t, ok)
	assert.Equal(t, int64(45231), port.Int())

	// Check log record
	require.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, "go.olly.garden/grafts/receiver/snmpreceiver", sl.Scope().Name())

	require.Equal(t, 1, sl.LogRecords().Len())
	lr := sl.LogRecords().At(0)

	assert.Equal(t, plog.SeverityNumberWarn, lr.SeverityNumber())
	assert.Contains(t, lr.Body().Str(), "1.3.6.1.6.3.1.1.5.3")

	// Check attributes
	trapOID, ok := lr.Attributes().Get("snmp.trap.oid")
	require.True(t, ok)
	assert.Equal(t, "1.3.6.1.6.3.1.1.5.3", trapOID.Str())

	trapType, ok := lr.Attributes().Get("snmp.trap.type")
	require.True(t, ok)
	assert.Equal(t, "linkDown", trapType.Str())

	version, ok := lr.Attributes().Get("snmp.trap.version")
	require.True(t, ok)
	assert.Equal(t, "v2c", version.Str())

	community, ok := lr.Attributes().Get("snmp.trap.community")
	require.True(t, ok)
	assert.Equal(t, "public", community.Str())
}

func TestBuildTrapLogColdStart(t *testing.T) {
	trap := TrapData{
		SourceIP: "10.0.0.1",
		SourcePort: 162,
		Version:  "v2c",
		TrapOID:  "1.3.6.1.6.3.1.1.5.1", // coldStart
		Timestamp: time.Now(),
	}

	ld := BuildLog(trap)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberInfo, lr.SeverityNumber())

	trapType, ok := lr.Attributes().Get("snmp.trap.type")
	require.True(t, ok)
	assert.Equal(t, "coldStart", trapType.Str())
}

func TestBuildTrapLogAuthFailure(t *testing.T) {
	trap := TrapData{
		SourceIP: "10.0.0.1",
		SourcePort: 162,
		Version:  "v3",
		TrapOID:  "1.3.6.1.6.3.1.1.5.5", // authenticationFailure
		Timestamp: time.Now(),
	}

	ld := BuildLog(trap)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberError, lr.SeverityNumber())
}

func TestBuildTrapLogEnterpriseOID(t *testing.T) {
	trap := TrapData{
		SourceIP: "10.0.0.1",
		SourcePort: 162,
		Version:  "v2c",
		TrapOID:  "1.3.6.1.4.1.9.9.999.1", // enterprise-specific
		Timestamp: time.Now(),
	}

	ld := BuildLog(trap)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	// Enterprise OIDs should not have snmp.trap.type
	_, ok := lr.Attributes().Get("snmp.trap.type")
	assert.False(t, ok)

	// Default severity for unknown traps
	assert.Equal(t, plog.SeverityNumberWarn, lr.SeverityNumber())
}

func TestBuildTrapLogVarbinds(t *testing.T) {
	trap := TrapData{
		SourceIP: "10.0.0.1",
		SourcePort: 162,
		Version:  "v2c",
		TrapOID:  "1.3.6.1.6.3.1.1.5.3",
		Varbinds: map[string]interface{}{
			"1.3.6.1.2.1.2.2.1.1": 2,
			"1.3.6.1.2.1.2.2.1.2": "FastEthernet0/1",
		},
		Timestamp: time.Now(),
	}

	ld := BuildLog(trap)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	vb1, ok := lr.Attributes().Get("snmp.varbind.1.3.6.1.2.1.2.2.1.1")
	require.True(t, ok)
	assert.Equal(t, "2", vb1.Str())

	vb2, ok := lr.Attributes().Get("snmp.varbind.1.3.6.1.2.1.2.2.1.2")
	require.True(t, ok)
	assert.Equal(t, "FastEthernet0/1", vb2.Str())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/logs/... -count=1`
Expected: Compilation errors.

- [ ] **Step 3: Write builder.go**

```go
package logs

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const scopeName = "go.olly.garden/grafts/receiver/snmpreceiver"

// TrapData contains parsed SNMP trap information.
type TrapData struct {
	SourceIP   string
	SourcePort int
	Version    string
	Community  string
	TrapOID    string
	Uptime     int64
	Varbinds   map[string]interface{}
	Timestamp  time.Time
}

// Well-known trap OIDs under 1.3.6.1.6.3.1.1.5.*
var wellKnownTraps = map[string]struct {
	Name     string
	Severity plog.SeverityNumber
}{
	"1.3.6.1.6.3.1.1.5.1": {Name: "coldStart", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.2": {Name: "warmStart", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.3": {Name: "linkDown", Severity: plog.SeverityNumberWarn},
	"1.3.6.1.6.3.1.1.5.4": {Name: "linkUp", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.5": {Name: "authenticationFailure", Severity: plog.SeverityNumberError},
}

// BuildLog constructs plog.Logs from a parsed SNMP trap.
func BuildLog(trap TrapData) plog.Logs {
	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("snmp.host", trap.SourceIP)
	rl.Resource().Attributes().PutInt("snmp.port", int64(trap.SourcePort))

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(scopeName)

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(trap.Timestamp))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Set severity based on well-known trap OID
	severity := plog.SeverityNumberWarn // default
	if known, ok := wellKnownTraps[trap.TrapOID]; ok {
		severity = known.Severity
		lr.Attributes().PutStr("snmp.trap.type", known.Name)
	}
	lr.SetSeverityNumber(severity)

	// Body
	lr.Body().SetStr(fmt.Sprintf("SNMP Trap: %s", trap.TrapOID))

	// Standard attributes
	lr.Attributes().PutStr("snmp.trap.oid", trap.TrapOID)
	lr.Attributes().PutStr("snmp.trap.version", trap.Version)
	if trap.Community != "" {
		lr.Attributes().PutStr("snmp.trap.community", trap.Community)
	}
	if trap.Uptime > 0 {
		lr.Attributes().PutInt("snmp.uptime", trap.Uptime)
	}

	// Varbinds
	for oid, val := range trap.Varbinds {
		lr.Attributes().PutStr("snmp.varbind."+oid, fmt.Sprintf("%v", val))
	}

	return ld
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/logs/... -count=1`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add receiver/snmpreceiver/internal/logs/
git commit -m "feat(snmpreceiver): add plog builder for SNMP trap data"
```

---

### Task 10: Trap listener and receiver wiring

**Files:**
- Create: `receiver/snmpreceiver/internal/trapper/trapper.go`
- Create: `receiver/snmpreceiver/internal/trapper/trapper_test.go`
- Modify: `receiver/snmpreceiver/receiver.go`
- Modify: `receiver/snmpreceiver/receiver_test.go`

- [ ] **Step 1: Write trapper_test.go**

```go
package trapper

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap/zaptest"
)

func TestTrapperStartStop(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	tr := New(logger, "127.0.0.1:0", []AuthEntry{{Version: "v2c", Community: "public"}}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tr.Run(ctx)
	}()

	// Give listener time to start
	require.Eventually(t, func() bool {
		return tr.ListenAddr() != ""
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestTrapperReceivesV2cTrap(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	tr := New(logger, "127.0.0.1:0", []AuthEntry{{Version: "v2c", Community: "public"}}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tr.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return tr.ListenAddr() != ""
	}, 2*time.Second, 50*time.Millisecond)

	// Send a v2c trap
	sendTestTrap(t, tr.ListenAddr(), "public", "1.3.6.1.6.3.1.1.5.3")

	// Wait for log to arrive
	require.Eventually(t, func() bool {
		return len(sink.AllLogs()) >= 1
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()

	logs := sink.AllLogs()
	require.Len(t, logs, 1)
	lr := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	trapOID, ok := lr.Attributes().Get("snmp.trap.oid")
	require.True(t, ok)
	assert.Equal(t, "1.3.6.1.6.3.1.1.5.3", trapOID.Str())
}

func TestTrapperRejectsWrongCommunity(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	tr := New(logger, "127.0.0.1:0", []AuthEntry{{Version: "v2c", Community: "secret"}}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tr.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return tr.ListenAddr() != ""
	}, 2*time.Second, 50*time.Millisecond)

	// Send trap with wrong community
	sendTestTrap(t, tr.ListenAddr(), "wrong", "1.3.6.1.6.3.1.1.5.3")

	// Give time for processing
	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	// Should have been rejected
	assert.Empty(t, sink.AllLogs())
}

func sendTestTrap(t *testing.T, addr, community, trapOID string) {
	t.Helper()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	var port uint16
	fmt.Sscanf(portStr, "%d", &port)

	g := &gosnmp.GoSNMP{
		Target:    host,
		Port:      port,
		Version:   gosnmp.Version2c,
		Community: community,
		Timeout:   time.Second,
	}
	err = g.Connect()
	require.NoError(t, err)
	defer g.Conn.Close()

	trap := gosnmp.SnmpTrap{
		Variables: []gosnmp.SnmpPDU{
			{Name: "1.3.6.1.2.1.1.3.0", Type: gosnmp.TimeTicks, Value: uint32(123456)},
			{Name: "1.3.6.1.6.3.1.1.4.1.0", Type: gosnmp.ObjectIdentifier, Value: trapOID},
		},
	}
	_, err = g.SendTrap(trap)
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/trapper/... -count=1`
Expected: Compilation errors.

- [ ] **Step 3: Write trapper.go**

```go
package trapper

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/gosnmp/gosnmp"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/logs"
)

const snmpTrapOIDMIB = "1.3.6.1.6.3.1.1.4.1.0"

// AuthEntry defines an accepted auth configuration for trap filtering.
type AuthEntry struct {
	Version   string
	Community string
	Username  string
}

// Trapper listens for SNMP traps and converts them to logs.
type Trapper struct {
	logger       *zap.Logger
	listenAddr   string
	acceptedAuth []AuthEntry
	consumer     consumer.Logs

	mu             sync.Mutex
	resolvedAddr   string
	listener       *gosnmp.TrapListener
}

// New creates a new Trapper.
func New(
	logger *zap.Logger,
	listenAddr string,
	acceptedAuth []AuthEntry,
	consumer consumer.Logs,
) *Trapper {
	return &Trapper{
		logger:       logger,
		listenAddr:   listenAddr,
		acceptedAuth: acceptedAuth,
		consumer:     consumer,
	}
}

// ListenAddr returns the resolved listen address (available after Run starts).
func (tr *Trapper) ListenAddr() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.resolvedAddr
}

// Run starts the trap listener. Blocks until ctx is cancelled.
func (tr *Trapper) Run(ctx context.Context) {
	listener := gosnmp.NewTrapListener()
	listener.Params = gosnmp.Default
	listener.OnNewTrap = tr.handleTrap

	tr.mu.Lock()
	tr.listener = listener
	tr.mu.Unlock()

	// Listen on UDP
	addr, err := net.ResolveUDPAddr("udp", tr.listenAddr)
	if err != nil {
		tr.logger.Error("Failed to resolve trap listen address", zap.Error(err))
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		tr.logger.Error("Failed to start trap listener", zap.Error(err))
		return
	}

	tr.mu.Lock()
	tr.resolvedAddr = conn.LocalAddr().String()
	tr.mu.Unlock()

	tr.logger.Info("SNMP trap listener started", zap.String("address", tr.resolvedAddr))

	// Run listener in background, close on context cancel
	done := make(chan struct{})
	go func() {
		defer close(done)
		listener.ListenUDP(conn)
	}()

	select {
	case <-ctx.Done():
		conn.Close()
		<-done
	case <-done:
	}

	tr.logger.Info("SNMP trap listener stopped")
}

func (tr *Trapper) handleTrap(packet *gosnmp.SnmpPacket, addr *net.UDPAddr) {
	// Auth check
	if !tr.isAuthorized(packet) {
		tr.logger.Warn("Rejected trap: unauthorized",
			zap.String("source", addr.String()),
			zap.String("community", packet.Community))
		return
	}

	// Extract trap OID and varbinds
	trapOID := ""
	varbinds := make(map[string]interface{})
	var uptime int64

	for _, v := range packet.Variables {
		switch v.Name {
		case snmpTrapOIDMIB:
			if oid, ok := v.Value.(string); ok {
				trapOID = oid
			}
		case "1.3.6.1.2.1.1.3.0": // sysUpTime
			if val, ok := v.Value.(uint32); ok {
				uptime = int64(val)
			}
		default:
			varbinds[v.Name] = v.Value
		}
	}

	if trapOID == "" {
		tr.logger.Warn("Trap missing snmpTrapOID", zap.String("source", addr.String()))
		return
	}

	version := "v2c"
	if packet.Version == gosnmp.Version3 {
		version = "v3"
	} else if packet.Version == gosnmp.Version1 {
		version = "v1"
	}

	trapData := logs.TrapData{
		SourceIP:   addr.IP.String(),
		SourcePort: addr.Port,
		Version:    version,
		Community:  packet.Community,
		TrapOID:    trapOID,
		Uptime:     uptime,
		Varbinds:   varbinds,
		Timestamp:  packet.Timestamp,
	}

	ld := logs.BuildLog(trapData)
	if err := tr.consumer.ConsumeLogs(context.Background(), ld); err != nil {
		tr.logger.Warn("Failed to send trap log to consumer", zap.Error(err))
	}
}

func (tr *Trapper) isAuthorized(packet *gosnmp.SnmpPacket) bool {
	for _, auth := range tr.acceptedAuth {
		switch auth.Version {
		case "v2c":
			if (packet.Version == gosnmp.Version2c || packet.Version == gosnmp.Version1) &&
				packet.Community == auth.Community {
				return true
			}
		case "v3":
			if packet.Version == gosnmp.Version3 {
				if sp, ok := packet.SecurityParameters.(*gosnmp.UsmSecurityParameters); ok {
					if sp.UserName == auth.Username {
						return true
					}
				}
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Run trapper tests to verify they pass**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/internal/trapper/... -count=1`
Expected: All tests PASS.

- [ ] **Step 5: Wire trapper into receiver.go**

Update the `if r.nextLogs != nil` block in `Start`:

```go
	if r.nextLogs != nil && r.config.TrapListener != nil {
		var authEntries []trapper.AuthEntry
		for _, authName := range r.config.TrapListener.AcceptedAuth {
			auth := r.config.Auth[authName]
			authEntries = append(authEntries, trapper.AuthEntry{
				Version:   auth.Version,
				Community: auth.Community,
				Username:  auth.Username,
			})
		}

		tr := trapper.New(r.settings.Logger, r.config.TrapListener.ListenAddress, authEntries, r.nextLogs)
		r.shutdownWG.Add(1)
		go func() {
			defer r.shutdownWG.Done()
			tr.Run(loopCtx)
		}()

		r.settings.Logger.Info("SNMP trap listener started",
			zap.String("address", r.config.TrapListener.ListenAddress))
	}
```

Add import for `"go.olly.garden/grafts/receiver/snmpreceiver/internal/trapper"`.

- [ ] **Step 6: Run all tests**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && go test -v ./receiver/snmpreceiver/... -count=1`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add receiver/snmpreceiver/internal/trapper/ receiver/snmpreceiver/receiver.go
git commit -m "feat(snmpreceiver): add trap listener and wire into receiver lifecycle"
```

---

### Task 11: Distribution integration

**Files:**
- Modify: `distributions/grafts/manifest.yaml`
- Modify: `distributions/grafts/config.yaml`

- [ ] **Step 1: Add snmpreceiver to manifest.yaml**

Add under the `receivers:` section, after the NATS JetStream receiver entry:

```yaml
  # SNMP receiver (from main module)
  - gomod: go.olly.garden/grafts v0.1.0
    import: go.olly.garden/grafts/receiver/snmpreceiver
    path: ../..
```

- [ ] **Step 2: Add sample snmpreceiver config to config.yaml**

Add an `snmp` receiver block in the receivers section and reference it in a pipeline. The exact config depends on the existing config.yaml structure -- add a commented-out sample section:

```yaml
  # SNMP receiver (uncomment and configure to use)
  # snmp:
  #   collection_interval: 60s
  #   auth:
  #     public_v2c:
  #       version: v2c
  #       community: public
  #   targets:
  #     - host: 192.168.1.1
  #       auth: public_v2c
  #       metric_groups: [system]
  #   metric_groups:
  #     system:
  #       metrics:
  #         - oid: "1.3.6.1.2.1.1.3.0"
  #           metric_name: snmp.system.uptime
  #           type: gauge
  #           unit: cs
```

- [ ] **Step 3: Build the distribution**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && make build`
Expected: Clean build.

- [ ] **Step 4: Commit**

```bash
git add distributions/grafts/manifest.yaml distributions/grafts/config.yaml
git commit -m "feat(snmpreceiver): add to grafts distribution"
```

---

### Task 12: Custom observability metrics (follow-up)

> **Note:** This task adds the SNMP-specific operational metrics defined in the spec (snmpreceiver.targets, snmpreceiver.poll.duration, snmpreceiver.poll.errors, etc.). It requires the `mdatagen` tool and a `metadata.yaml` file. This is best done as a follow-up after the core functionality is stable, since it adds instrumentation throughout the poller, trapper, and receiver code. Create a separate PR for this.

The `metadata.yaml` should define:
- Component type and stability
- All custom metrics from the spec's "Observability" section
- Run `mdatagen` to generate the telemetry builder

Then instrument:
- `internal/poller/poller.go` -- poll duration histogram, target state, walk/get counters
- `internal/trapper/trapper.go` -- traps received/rejected counters
- `receiver.go` -- connection state, consumer error counters

---

### Task 13: Final verification

- [ ] **Step 1: Run all tests**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && make test`
Expected: All tests PASS across all components.

- [ ] **Step 2: Run linter**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && make lint`
Expected: No lint errors.

- [ ] **Step 3: Build distribution**

Run: `cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && make build`
Expected: Clean build with snmpreceiver included.
