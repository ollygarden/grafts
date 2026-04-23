# SNMP Receiver — Manual Runbook + Integration Test

**Status:** Design approved 2026-04-23
**Scope:** Add a manual testing runbook and an automated integration test for `receiver/snmpreceiver`, both using [snmpsim](https://github.com/lextudio/snmpsim) as the simulated SNMP agent.

## Goals

1. A developer can smoke-test the SNMP receiver end-to-end in under five minutes using copy-pasteable commands.
2. CI (and local `make test-integration`) can run a headless integration test that starts snmpsim in a container, points the receiver at it, and asserts on emitted metrics.
3. Both SNMPv2c and SNMPv3 (AuthPriv) polling paths are exercised.

Out of scope: trap-listener testing (separate follow-up), performance/load testing, chaos testing.

## Phase 1 — Manual Runbook

### Deliverables

- `receiver/snmpreceiver/TESTING.md` — the runbook. Covers prerequisites, snmpsim launch, collector config, how to run the collector, expected output, and troubleshooting.
- `receiver/snmpreceiver/testdata/local/config.yaml` — ready-to-run collector config pointing at snmpsim on `localhost:1161` with two targets (v2c + v3), two metric groups (`system` scalar + `interfaces` table walk), and the `debug` exporter at `detailed` verbosity.
- Link from `receiver/snmpreceiver/README.md` to `TESTING.md` (new "Running locally" section with a one-line pointer).

### snmpsim setup

Image: `ghcr.io/lextudio/snmpsim:latest`.

Run command (from the runbook):

```bash
docker run --rm -d --name snmpsim \
  -p 1161:1161/udp \
  ghcr.io/lextudio/snmpsim:latest \
  --agent-udpv4-endpoint=0.0.0.0:1161
```

snmpsim ships with a default `public` recording under community `public` covering standard MIB-II OIDs (`sysUpTime.0`, `ifTable`, `sysName.0`, etc.) — enough for both scalar and table walks with no additional fixture files.

For SNMPv3, snmpsim's built-in `simulator` user profile (MD5 auth + DES priv, passphrase `auctoritas`/`privatus` in default recordings) is used. The runbook documents these literal credentials with a note that they're simulator defaults, not production.

### Collector config structure

```yaml
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

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [snmp]
      exporters: [debug]
```

### Run steps (as documented in TESTING.md)

1. Start snmpsim (one `docker run` command above).
2. Build the distribution: `cd distributions/grafts && make build`.
3. Run the collector: `./build/grafts --config=../../receiver/snmpreceiver/testdata/local/config.yaml`.
4. Verify the debug exporter logs metric records naming `snmp.system.uptime` and `snmp.interface.in_octets` with `snmp.host=127.0.0.1` resource attributes.
5. Stop snmpsim: `docker stop snmpsim`.

### Troubleshooting section

Covers: UDP port 1161 already in use, container failing to bind, v3 auth failures (usually typo in passphrase), empty metrics (check community string and that snmpsim container is healthy via `docker logs snmpsim`).

## Phase 2 — Integration Test

### Deliverables

- `receiver/snmpreceiver/integration_test.go` with `//go:build integration` build tag.
- `make test-integration` target at the repo root (and in `receiver/snmpreceiver/Makefile` if one exists), running `go test -tags=integration -timeout=120s ./...`.
- A dev dependency on `github.com/testcontainers/testcontainers-go` added only to `receiver/snmpreceiver/go.mod`.

### Test structure

Single top-level `TestIntegration_Polling` with two subtests (`v2c`, `v3`). Both share a single snmpsim container started in `TestMain` (or a `testcontainers.GenericContainer` with a cleanup hook on the test).

Flow:
1. Start snmpsim container, expose UDP 1161 to a random host port.
2. Wait for readiness: poll `sysUpTime.0` directly with `gosnmp` in a 10s backoff loop until it returns a value. (snmpsim has no HTTP health endpoint, so we probe via the protocol itself.)
3. For each subtest: build a receiver config pointed at `localhost:<mappedPort>` with the appropriate auth; start the receiver against a `consumertest.MetricsSink`; wait up to 30s for the sink to accumulate at least one `ResourceMetrics` from the target.
4. Assertions (loose on values, strict on shape):
   - At least one metric named `snmp.system.uptime` of type gauge.
   - At least one metric named `snmp.interface.in_octets` with ≥1 data point carrying an `interfaces_index` attribute (v2c subtest only, since the v3 target uses only the `system` group).
   - Resource attribute `snmp.host` present.
5. Shut down receiver; container is torn down by `t.Cleanup`.

### Environment compatibility

testcontainers-go requires a Docker-API-compatible runtime. Works with Docker Desktop, Colima, OrbStack, Rancher Desktop, and plain Docker on Linux CI. UDP port mapping is supported by all of these; macOS Docker Desktop has had historical UDP quirks but is reliable for localhost-bound tests of this kind.

If no Docker socket is reachable, the test is skipped with `t.Skip("docker unavailable")` rather than failed, so `make test-integration` is safe to run in environments without Docker.

### CI

Not wired into CI in this change — that's a follow-up once the test is proven stable locally. The new make target exists so CI can opt in later with a one-line job addition.

## File Layout Summary

```
receiver/snmpreceiver/
  README.md                          # add "Running locally" pointer section
  TESTING.md                         # new — manual runbook
  integration_test.go                # new — //go:build integration
  testdata/
    local/
      config.yaml                    # new — runbook config
```

Plus a `test-integration` target added to the root `Makefile`.

## Risks and Mitigations

- **snmpsim image drift:** Pin to a specific tag (not `latest`) in both the runbook and the integration test once we pick one that works. Design leaves tag as `latest` for initial development; the implementation plan should bake in a version pin.
- **Flaky UDP readiness:** Protocol-level probing in the test (rather than `time.Sleep`) and a generous 30s data-collection window.
- **Credential leakage concern:** The v3 credentials are snmpsim defaults and publicly documented; no secret handling needed. README/TESTING.md explicitly labels them as test-only.
