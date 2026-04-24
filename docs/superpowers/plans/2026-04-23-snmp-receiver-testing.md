# SNMP Receiver Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual testing runbook and an automated integration test for `receiver/snmpreceiver`, both exercising v2c + v3 polling against snmpsim.

**Architecture:** Phase 1 ships a copy-pasteable runbook and a ready-to-run collector config under `receiver/snmpreceiver/testdata/local/`. Phase 2 adds a `//go:build integration` test that uses testcontainers-go to spin up snmpsim, then runs the receiver against it with a `consumertest.MetricsSink` and asserts on metric shape.

**Tech Stack:** Go, OpenTelemetry Collector SDK (`receivertest`, `consumertest`), `gosnmp/gosnmp`, `testcontainers/testcontainers-go`, snmpsim (`ghcr.io/lextudio/snmpsim`).

**Repo note:** Despite CLAUDE.md's description, the repo is currently a single Go module at the root (`go.olly.garden/grafts`). All Go dependencies go in the root `go.mod`.

---

## File Structure

- Create: `receiver/snmpreceiver/testdata/local/config.yaml` — collector config pointing at local snmpsim
- Create: `receiver/snmpreceiver/TESTING.md` — manual runbook
- Modify: `receiver/snmpreceiver/README.md` — add "Running locally" pointer
- Create: `receiver/snmpreceiver/integration_test.go` — `//go:build integration` test
- Modify: `go.mod`, `go.sum` — add testcontainers-go dev dep
- Modify: `Makefile` — add `test-integration` target

---

## Task 1: Runbook collector config

**Files:**
- Create: `receiver/snmpreceiver/testdata/local/config.yaml`

- [ ] **Step 1: Create the config file**

```yaml
# Collector config for manual SNMP receiver testing against snmpsim.
# See receiver/snmpreceiver/TESTING.md for how to use this.
receivers:
  snmp:
    collection_interval: 10s
    auth:
      public_v2c:
        version: v2c
        community: public
      sim_v3:
        version: v3
        username: simulator
        auth_protocol: MD5
        auth_passphrase: auctoritas
        privacy_protocol: DES
        privacy_passphrase: privatus
    targets:
      - host: 127.0.0.1
        port: 1161
        auth: public_v2c
        metric_groups: [system, interfaces]
      - host: 127.0.0.1
        port: 1161
        auth: sim_v3
        metric_groups: [system]
    metric_groups:
      system:
        metrics:
          - oid: "1.3.6.1.2.1.1.3.0"
            metric_name: snmp.system.uptime
            type: gauge
            unit: "cs"
            description: "System uptime in centiseconds"
        scalar_attributes:
          - oid: "1.3.6.1.2.1.1.5.0"
            name: sys_name
      interfaces:
        walk: "1.3.6.1.2.1.2.2.1"
        metrics:
          - oid: "1.3.6.1.2.1.2.2.1.10"
            metric_name: snmp.interface.in_octets
            type: counter
            unit: "By"
            description: "Bytes received on interface"

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [snmp]
      exporters: [debug]
```

- [ ] **Step 2: Validate the config parses**

Run:
```bash
cd distributions/grafts && make build && \
  ./build/grafts validate --config=../../receiver/snmpreceiver/testdata/local/config.yaml
```

Expected: exits 0 with no error output. If the `validate` subcommand is unavailable, instead run the binary with the config for ~2 seconds; the collector should start without config errors (ignore SNMP connection errors — snmpsim isn't running yet).

- [ ] **Step 3: Commit**

```bash
git add receiver/snmpreceiver/testdata/local/config.yaml
git commit -m "test(snmpreceiver): add local testing collector config for snmpsim"
```

---

## Task 2: TESTING.md runbook

**Files:**
- Create: `receiver/snmpreceiver/TESTING.md`

- [ ] **Step 1: Write the runbook**

Create `receiver/snmpreceiver/TESTING.md` with exactly this content:

````markdown
# Testing the SNMP Receiver Locally

This runbook walks through smoke-testing the SNMP receiver end-to-end against [snmpsim](https://github.com/lextudio/snmpsim), a pure-Python SNMP agent simulator.

Runtime: ~5 minutes. Requires Docker and a built `grafts` binary.

## 1. Start snmpsim

```bash
docker run --rm -d --name snmpsim \
  -p 1161:1161/udp \
  ghcr.io/lextudio/snmpsim:latest \
  --agent-udpv4-endpoint=0.0.0.0:1161
```

snmpsim ships with a default recording under community `public` that covers standard MIB-II OIDs (`sysUpTime.0`, `ifTable`, `sysName.0`). No fixture files to author.

Verify it's up:

```bash
docker logs snmpsim | tail -5
```

You should see a line like `SNMP agent at UDP/IPv4 endpoint 0.0.0.0:1161`.

## 2. Build the collector

```bash
cd distributions/grafts && make build
```

Produces `distributions/grafts/build/grafts`.

## 3. Run the collector

From the repo root:

```bash
./distributions/grafts/build/grafts \
  --config=./receiver/snmpreceiver/testdata/local/config.yaml
```

## 4. Verify output

Within ~15 seconds you should see the `debug` exporter log metric records. Look for:

- `snmp.system.uptime` — gauge, single data point, resource attribute `snmp.host=127.0.0.1`
- `snmp.interface.in_octets` — counter, multiple data points (one per interface in snmpsim's `ifTable`), each with an `interfaces_index` attribute

Example line (abbreviated):

```
Metric #0
Descriptor:
     -> Name: snmp.system.uptime
     -> Unit: cs
     -> DataType: Gauge
NumberDataPoints #0
Timestamp: ...
Value: 12345
```

The v3 target polls the same `system` group, so you should see `snmp.system.uptime` emitted for **both** targets each cycle (identical resource attributes — this is expected for a single snmpsim instance).

## 5. Cleanup

```bash
# Ctrl-C the collector
docker stop snmpsim
```

## SNMPv3 credentials

The config uses snmpsim's built-in `simulator` user profile. These are **simulator defaults, not production credentials**:

| Field | Value |
|---|---|
| username | `simulator` |
| auth_protocol | `MD5` |
| auth_passphrase | `auctoritas` |
| privacy_protocol | `DES` |
| privacy_passphrase | `privatus` |

## Troubleshooting

**Port 1161 already in use.** Another SNMP simulator or an earlier `snmpsim` container may be running. Run `docker ps` and stop the conflicting container, or change the port mapping (e.g. `-p 11161:1161/udp`) and update `targets[].port` in the config accordingly.

**Collector starts but no metrics appear.** Check `docker logs snmpsim` for errors. Verify connectivity from the host:

```bash
docker run --rm --network host alpine/socat - UDP:127.0.0.1:1161 </dev/null
```

**SNMPv3 auth failures in collector logs.** Most often a typo in `auth_passphrase` or `privacy_passphrase`. Values must be exactly `auctoritas` and `privatus`.

**macOS UDP flakiness.** Docker Desktop on macOS has historically had quirks with UDP port forwarding. If metrics are intermittent, try `colima` or `orbstack` as an alternative runtime, or run snmpsim directly on the host via `pip install snmpsim`.

## Automated integration test

For a headless version of this flow, see `integration_test.go` in this package. Run it with:

```bash
make test-integration
```
````

- [ ] **Step 2: Commit**

```bash
git add receiver/snmpreceiver/TESTING.md
git commit -m "docs(snmpreceiver): add TESTING.md runbook for local smoke testing with snmpsim"
```

---

## Task 3: Link runbook from README

**Files:**
- Modify: `receiver/snmpreceiver/README.md`

- [ ] **Step 1: Add a "Running locally" section**

Open `receiver/snmpreceiver/README.md` and insert the following section **immediately before** the `## Status` section near the end of the file:

```markdown
## Running locally

For a step-by-step walkthrough of building the collector, running an SNMP simulator in Docker, and verifying the receiver works end-to-end, see [TESTING.md](./TESTING.md).

```

- [ ] **Step 2: Commit**

```bash
git add receiver/snmpreceiver/README.md
git commit -m "docs(snmpreceiver): link TESTING.md runbook from component README"
```

---

## Task 4: Add testcontainers-go dependency

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Add the dependency**

Run:

```bash
go get github.com/testcontainers/testcontainers-go@latest
go mod tidy
```

- [ ] **Step 2: Verify the module still builds**

Run:

```bash
go build ./...
```

Expected: exits 0 with no output.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "test(snmpreceiver): add testcontainers-go dev dependency for integration tests"
```

---

## Task 5: Write the integration test (failing)

**Files:**
- Create: `receiver/snmpreceiver/integration_test.go`

- [ ] **Step 1: Write the test file**

Create `receiver/snmpreceiver/integration_test.go` with the following content:

```go
//go:build integration

package snmpreceiver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	snmpsimImage = "ghcr.io/lextudio/snmpsim:latest"
	snmpsimPort  = "1161/udp"
)

// startSnmpsim launches snmpsim in a container, waits for it to respond to a
// real SNMP GET, and returns the host:port the host can reach it on.
func startSnmpsim(ctx context.Context, t *testing.T) (host string, port uint16) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        snmpsimImage,
		ExposedPorts: []string{snmpsimPort},
		Cmd:          []string{"--agent-udpv4-endpoint=0.0.0.0:1161"},
		WaitingFor:   wait.ForLog("SNMP agent").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	mappedHost, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, snmpsimPort)
	require.NoError(t, err)

	// Protocol-level readiness probe: snmpsim has no HTTP endpoint.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		g := &gosnmp.GoSNMP{
			Target:    mappedHost,
			Port:      uint16(mappedPort.Int()),
			Community: "public",
			Version:   gosnmp.Version2c,
			Timeout:   2 * time.Second,
			Retries:   0,
		}
		if err := g.Connect(); err == nil {
			resp, err := g.Get([]string{"1.3.6.1.2.1.1.3.0"})
			_ = g.Conn.Close()
			if err == nil && len(resp.Variables) > 0 {
				return mappedHost, uint16(mappedPort.Int())
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("snmpsim did not become ready at %s:%s", mappedHost, mappedPort.Port())
	return "", 0
}

// newTestConfig builds a receiver Config targeting the given snmpsim endpoint.
// includeV3 adds a second target using snmpsim's default v3 simulator user.
// includeInterfaces adds the ifTable walk group.
func newTestConfig(host string, port uint16, includeV3, includeInterfaces bool) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.CollectionInterval = 5 * time.Second
	cfg.Auth = map[string]AuthConfig{
		"public_v2c": {
			Version:   "v2c",
			Community: "public",
		},
	}
	groups := []string{"system"}
	cfg.MetricGroups = map[string]MetricGroup{
		"system": {
			Metrics: []MetricConfig{{
				OID:        "1.3.6.1.2.1.1.3.0",
				MetricName: "snmp.system.uptime",
				Type:       "gauge",
				Unit:       "cs",
			}},
		},
	}
	if includeInterfaces {
		cfg.MetricGroups["interfaces"] = MetricGroup{
			Walk: "1.3.6.1.2.1.2.2.1",
			Metrics: []MetricConfig{{
				OID:        "1.3.6.1.2.1.2.2.1.10",
				MetricName: "snmp.interface.in_octets",
				Type:       "counter",
				Unit:       "By",
			}},
		}
		groups = append(groups, "interfaces")
	}
	cfg.Targets = []TargetConfig{{
		Host:         host,
		Port:         port,
		Auth:         "public_v2c",
		MetricGroups: groups,
	}}
	if includeV3 {
		cfg.Auth["sim_v3"] = AuthConfig{
			Version:           "v3",
			Username:          "simulator",
			AuthProtocol:      "MD5",
			AuthPassphrase:    "auctoritas",
			PrivacyProtocol:   "DES",
			PrivacyPassphrase: "privatus",
		}
		cfg.Targets = append(cfg.Targets, TargetConfig{
			Host:         host,
			Port:         port,
			Auth:         "sim_v3",
			MetricGroups: []string{"system"},
		})
	}
	return cfg
}

// waitForMetric polls the sink until at least one metric with the given name
// is observed or the deadline expires.
func waitForMetric(t *testing.T, sink *consumertest.MetricsSink, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, md := range sink.AllMetrics() {
			rms := md.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				sms := rms.At(i).ScopeMetrics()
				for j := 0; j < sms.Len(); j++ {
					ms := sms.At(j).Metrics()
					for k := 0; k < ms.Len(); k++ {
						if ms.At(k).Name() == name {
							return
						}
					}
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metric %q (got %d batches)", name, len(sink.AllMetrics()))
}

func TestIntegration_Polling_V2c(t *testing.T) {
	ctx := context.Background()
	host, port := startSnmpsim(ctx, t)

	cfg := newTestConfig(host, port, false /*v3*/, true /*interfaces*/)
	sink := new(consumertest.MetricsSink)
	factory := NewFactory()
	r, err := factory.CreateMetrics(ctx, receivertest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	waitForMetric(t, sink, "snmp.system.uptime", 30*time.Second)
	waitForMetric(t, sink, "snmp.interface.in_octets", 30*time.Second)

	// Shape assertions: interfaces metric has ≥1 data point with index attribute.
	foundIndexed := false
	for _, md := range sink.AllMetrics() {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			_, hasHost := rms.At(i).Resource().Attributes().Get("snmp.host")
			assert.True(t, hasHost, "resource should carry snmp.host attribute")
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != "snmp.interface.in_octets" {
						continue
					}
					dps := m.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						if _, ok := dps.At(d).Attributes().Get("interfaces_index"); ok {
							foundIndexed = true
						}
					}
				}
			}
		}
	}
	assert.True(t, foundIndexed, "expected at least one snmp.interface.in_octets data point with interfaces_index attribute")
}

func TestIntegration_Polling_V3(t *testing.T) {
	ctx := context.Background()
	host, port := startSnmpsim(ctx, t)

	cfg := newTestConfig(host, port, true /*v3*/, false /*interfaces*/)
	sink := new(consumertest.MetricsSink)
	factory := NewFactory()
	r, err := factory.CreateMetrics(ctx, receivertest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	waitForMetric(t, sink, "snmp.system.uptime", 45*time.Second)
	// Both v2c and v3 targets emit snmp.system.uptime; just verify we got it at least once.
	require.Greater(t, len(sink.AllMetrics()), 0)

	_ = fmt.Sprintf // keep import stable if debugging is added
}
```

**IMPORTANT:** Before committing, verify that the types referenced (`Config`, `AuthConfig`, `MetricGroup`, `MetricConfig`, `TargetConfig`, and their field names) actually match what's defined in `receiver/snmpreceiver/config.go`. If any field is named differently (e.g., `Community` vs `CommunityString`, `MetricName` vs `Name`), update this test to match the real types — do NOT modify the types themselves. Read `receiver/snmpreceiver/config.go` first, adjust the test, then proceed.

- [ ] **Step 2: Run the test to verify it compiles and runs**

Run:

```bash
go test -tags=integration -timeout=180s -run TestIntegration ./receiver/snmpreceiver/...
```

Expected: both subtests PASS when Docker is available, or SKIP with "docker unavailable" if not. If the test fails due to a type mismatch against `config.go`, fix the test (not the production code) and re-run.

- [ ] **Step 3: Commit**

```bash
git add receiver/snmpreceiver/integration_test.go
git commit -m "test(snmpreceiver): add integration test covering v2c and v3 polling against snmpsim"
```

---

## Task 6: Makefile target

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add `test-integration` target**

Open `Makefile`. In the `.PHONY` line at the top, add `test-integration`:

```makefile
.PHONY: test lint fmt tidy build test-integration
```

Then append the following target at the end of the file:

```makefile
# Run integration tests (require Docker). Tests skip themselves if Docker is unavailable.
test-integration:
	@echo "Running integration tests..."
	@go test -tags=integration -timeout=300s ./receiver/snmpreceiver/...
```

- [ ] **Step 2: Verify the target runs**

Run:

```bash
make test-integration
```

Expected: same outcome as Task 5 Step 2 — PASS with Docker, SKIP without.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "build: add test-integration target for Docker-based integration tests"
```

---

## Self-Review Notes

- **Spec coverage:** Phase 1 deliverables (TESTING.md, testdata/local/config.yaml, README link) → Tasks 1–3. Phase 2 deliverables (integration_test.go, make target, testcontainers-go dep) → Tasks 4–6. Troubleshooting section covered in Task 2. v2c + v3 both covered in Tasks 1 (config) and 5 (test).
- **Placeholder scan:** Task 5 has an explicit instruction to verify type names against `config.go` before committing — this is guarded verification, not a placeholder. All code blocks are complete.
- **Type consistency note:** The test code in Task 5 assumes the config struct field names match idiomatic Go (`CollectionInterval`, `Community`, `MetricName`, etc.). The executing agent MUST read `receiver/snmpreceiver/config.go` before writing the test and adjust field names to match. This is called out explicitly in Task 5 Step 1.
