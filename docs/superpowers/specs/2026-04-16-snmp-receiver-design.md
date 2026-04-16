# SNMP Receiver Design Spec

## Overview

A unified OpenTelemetry Collector receiver (`snmpreceiver`) for OllyGarden that polls SNMP targets for metrics and listens for SNMP traps/informs as logs. Ships as a grafted component in the `go.olly.garden/grafts` module.

### Signals

- **Polling** produces **metrics** (via `consumer.Metrics`)
- **Trap listening** produces **logs** (via `consumer.Logs`)
- Either or both can be configured independently

### SNMP support

- SNMPv2c (community string authentication)
- SNMPv3 (USM: username + auth protocol + privacy protocol)
- SNMPv1 traps are accepted and normalized to v2c format (RFC 2576)

### Library

- `gosnmp/gosnmp` -- pure Go, no CGo, supports v1/v2c/v3, GET/GETBULK/WALK, trap listening

## Configuration

```yaml
snmpreceiver:
  # Polling interval for all targets
  collection_interval: 60s

  # Named auth configurations (reusable across targets)
  auth:
    public_v2c:
      version: v2c
      community: public
    secure_v3:
      version: v3
      username: monitor
      auth_protocol: SHA
      auth_passphrase: ${env:SNMP_AUTH_PASS}
      privacy_protocol: AES
      privacy_passphrase: ${env:SNMP_PRIV_PASS}

  # Targets to poll (metrics signal)
  targets:
    - host: 192.168.1.1
      port: 161              # default: 161
      auth: public_v2c
      metric_groups: [if_traffic, system]
    - host: 192.168.1.2
      auth: secure_v3
      metric_groups: [if_traffic]

  # Named metric groups (collection profiles per device type)
  metric_groups:
    if_traffic:
      walk: "1.3.6.1.2.1.2.2.1"    # ifTable subtree to WALK
      metrics:
        - oid: "1.3.6.1.2.1.2.2.1.10"
          metric_name: snmp.interface.in_octets
          type: counter
          unit: "By"
          description: "Bytes received on interface"
        - oid: "1.3.6.1.2.1.2.2.1.16"
          metric_name: snmp.interface.out_octets
          type: counter
          unit: "By"
          description: "Bytes sent on interface"
      attributes:
        - oid: "1.3.6.1.2.1.2.2.1.2"
          name: interface_description
      lookups:
        - source_indexes: [interface_index]
          lookup_oid: "1.3.6.1.2.1.31.1.1.1.1"
          target_label: interface_name
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

  # Trap/inform listener (logs signal)
  trap_listener:
    listen_address: 0.0.0.0:162
    accepted_auth: [public_v2c, secure_v3]

  # Connection tuning
  timeout: 5s
  retries: 2
  max_repetitions: 25       # GETBULK max-repetitions for v2c/v3 walks
```

### Design decisions

1. **Named auth blocks.** Define once, reference by name per target. Avoids credential duplication. Inspired by Prometheus SNMP Exporter. Auth is inline (not an extension) because SNMP auth is protocol-specific -- the collector's auth extensions are HTTP/gRPC-based and don't fit.

2. **Metric groups as reusable profiles.** A group defines related OIDs to collect together. Targets reference groups by name. Same group reuses across many targets (e.g., "if_traffic" works for any device with standard IF-MIB). Inspired by Prometheus modules.

3. **Walk vs scalar.** If a group has `walk:`, the metrics and attributes within it are table-based (indexed via SNMP WALK/BULKWALK). If no `walk:`, metrics are scalars fetched with GET.

4. **Lookups.** Resolve raw table indexes to human-readable labels. `source_indexes` names which index field(s) to resolve, `lookup_oid` is the table to walk for values, `target_label` is the resulting attribute name. Supports chained lookups.

5. **Metric types.** `counter` maps to monotonic Sum, `gauge` maps to Gauge, `up_down_counter` maps to non-monotonic Sum. User must specify the type per OID.

6. **Static OID list.** Users specify numeric OIDs directly. No MIB parsing at runtime. A companion generator tool could be added later to produce configs from MIBs.

7. **Trap listener is optional.** If `trap_listener` is omitted or no logs pipeline references the receiver, no UDP listener starts.

## File Structure

```
receiver/snmpreceiver/
├── doc.go
├── config.go               # Config structs + Validate()
├── config_test.go
├── factory.go              # NewFactory, shared receiver (LoadOrStore pattern)
├── factory_test.go
├── receiver.go             # Orchestrator: lifecycle, wires poller + trapper
├── receiver_test.go
├── metadata.yaml           # mdatagen: component metadata + custom telemetry
├── internal/
│   ├── connection/
│   │   ├── connection.go       # Connection interface + gosnmp wrapper
│   │   └── connection_test.go
│   ├── poller/
│   │   ├── poller.go           # Poll scheduler, per-target goroutines
│   │   ├── poller_test.go
│   │   ├── collector.go        # SNMP GET/WALK, metric group collection
│   │   └── collector_test.go
│   ├── trapper/
│   │   ├── trapper.go          # Trap listener, v1-to-v2 normalization
│   │   └── trapper_test.go
│   ├── metrics/
│   │   ├── builder.go          # pmetric construction from SNMP responses
│   │   └── builder_test.go
│   └── logs/
│       ├── builder.go          # plog construction from trap PDUs
│       └── builder_test.go
└── testdata/
    ├── config.yaml
    ├── config_invalid_*.yaml
    └── ...
```

### Structure rationale

Follows the OTLP receiver's `internal/` pattern:
- Top-level `receiver.go` is the orchestrator (like `otlp.go`) -- owns Start/Shutdown, wires internal components
- `internal/poller/` and `internal/trapper/` are signal-specific handlers (like `internal/trace/`, `internal/logs/`)
- `internal/connection/` is shared SNMP transport infrastructure (like `internal/errors/`)
- `internal/metrics/` and `internal/logs/` are pure pdata builders -- no I/O, independently testable
- `metadata.yaml` + mdatagen generates component metadata and custom telemetry builder

## Data Model

### Polled metrics -> pmetric

Each poll cycle for a target produces a `pmetric.Metrics`:

```
pmetric.Metrics
└── ResourceMetrics
    ├── Resource:
    │   ├── snmp.host = "192.168.1.1"
    │   ├── snmp.port = 161
    │   └── sys_name = "core-switch-01"    (from scalar_attributes)
    └── ScopeMetrics
        ├── Scope:
        │   ├── name = "go.olly.garden/grafts/receiver/snmpreceiver"
        │   └── version = "0.1.0"
        └── Metrics:
            ├── Metric: snmp.interface.in_octets
            │   ├── type: Sum (monotonic, cumulative)
            │   ├── unit: "By"
            │   ├── description: "Bytes received on interface"
            │   └── DataPoints:
            │       ├── {interface_name="eth0", interface_index="1"} = 1234567890
            │       └── {interface_name="eth1", interface_index="2"} = 9876543210
            └── Metric: snmp.interface.out_octets
                ├── type: Sum (monotonic, cumulative)
                ├── unit: "By"
                └── DataPoints:
                    ├── {interface_name="eth0", interface_index="1"} = 987654321
                    └── {interface_name="eth1", interface_index="2"} = 123456789
```

#### Metric type mapping

| Config `type` | OTel type | Monotonic | Temporality |
|---|---|---|---|
| `counter` | Sum | yes | Cumulative |
| `gauge` | Gauge | n/a | n/a |
| `up_down_counter` | Sum | no | Cumulative |

#### Table walk -> data points

When walking a table OID, each index suffix becomes a separate data point. The raw index is always included as an attribute named `<metric_group_name>_index` (e.g., for group `if_traffic`, the index attribute is `if_traffic_index`). Lookups resolve the raw index to human-readable labels.

#### Resource attributes

Each target (host+port) gets its own `ResourceMetrics`. Scalar attributes (`scalar_attributes`) from the target's metric groups are attached to the Resource via a one-time GET at startup. Periodic refresh of scalar attributes is out of scope for the initial implementation.

#### Timestamps

Each data point gets the collection timestamp (when the SNMP response was received).

### Traps -> plog

Each incoming trap/inform produces a `plog.LogRecord`:

```
plog.Logs
└── ResourceLogs
    ├── Resource:
    │   ├── snmp.host = "192.168.1.100"     (source IP of trap sender)
    │   └── snmp.port = 45231               (source port)
    └── ScopeLogs
        ├── Scope:
        │   ├── name = "go.olly.garden/grafts/receiver/snmpreceiver"
        │   └── version = "0.1.0"
        └── LogRecords:
            └── LogRecord:
                ├── timestamp = 2026-04-16T10:30:00Z
                ├── observed_timestamp = 2026-04-16T10:30:00.123Z
                ├── severity = WARN
                ├── body = "SNMP Trap: 1.3.6.1.6.3.1.1.5.3"
                └── attributes:
                    ├── snmp.trap.oid = "1.3.6.1.6.3.1.1.5.3"
                    ├── snmp.trap.type = "linkDown"
                    ├── snmp.trap.version = "v2c"
                    ├── snmp.trap.community = "public"
                    ├── snmp.uptime = 123456
                    ├── snmp.varbind.1.3.6.1.2.1.2.2.1.1 = "2"
                    └── snmp.varbind.1.3.6.1.2.1.2.2.1.7 = "2"
```

#### Trap mapping rules

- **Resource:** Source IP/port of the trap sender. One resource per unique sender.
- **Severity:** Default WARN. Well-known trap OIDs get mapped severity (linkDown -> WARN, authenticationFailure -> ERROR, coldStart -> INFO). Configurable override possible later.
- **Body:** Human-readable string with trap OID.
- **Trap OID:** The SNMPv2-MIB::snmpTrapOID value, stored in `snmp.trap.oid`.
- **Varbinds:** Each variable binding becomes a log attribute prefixed with `snmp.varbind.`. Values are coerced to strings/ints based on SNMP type.
- **v1 normalization:** SNMPv1 traps are converted to v2c format per RFC 2576 before processing.
- **Trap type detection:** Standard well-known trap OIDs under `1.3.6.1.6.3.1.1.5.*` get a `snmp.trap.type` attribute with the human-readable name.

## Architecture

### Polling data flow

```
receiver.go (orchestrator)
  └── internal/poller/
        ├── poller.go: starts goroutine per target with ticker(collection_interval)
        └── collector.go: for each metric_group
              ├── WALK or GET via Connection interface
              │     └── internal/connection/ (gosnmp wrapper)
              │           └── SNMP device
              └── response -> internal/metrics/builder.go -> pmetric.Metrics
                    └── consumer.ConsumeMetrics()
```

### Trap data flow

```
receiver.go (orchestrator)
  └── internal/trapper/
        ├── trapper.go: UDP listener on listen_address
        │     ├── auth check (community/v3 user)
        │     └── v1? normalize to v2 (RFC 2576)
        └── internal/logs/builder.go -> plog.Logs
              └── consumer.ConsumeLogs()
```

### Lifecycle

```
Start(ctx, host)
  ├── if metrics consumer registered:
  │     ├── for each target: create Connection (gosnmp)
  │     ├── resolve scalar_attributes (one-time GET per target)
  │     └── start poller goroutine per target (ticker loop)
  ├── if logs consumer registered:
  │     └── start trap listener (UDP)
  └── return nil (non-blocking)

Shutdown(ctx)
  ├── cancel context (signals all goroutines)
  ├── stop trap listener
  ├── close all SNMP connections
  ├── shutdownWG.Wait()
  └── return nil
```

### Shared receiver pattern

Uses the same `sharedReceiver` + `receiverWrapper` + `sync.Once` pattern as `natsjetstreamreceiver`. A single receiver instance handles both metrics and logs consumers. The factory uses `getOrCreateReceiver` keyed by component ID.

### Error handling

| Scenario | Behavior |
|---|---|
| Target unreachable on poll | Log warning, skip this cycle, retry next interval |
| Single OID walk fails | Log error with OID + target, continue with remaining groups |
| Trap auth rejected | Log warning with source IP, drop the trap |
| Trap parse failure | Log error, drop the trap |
| Consumer returns retryable error | Log warning, data is lost (SNMP is best-effort, no replay) |
| Consumer returns permanent error | Log error, data is lost |
| Connection lost mid-cycle | gosnmp handles reconnect on next poll, log warning |

## Observability

### Built-in metrics (from receiverhelper.ObsReport)

Provided automatically by the collector framework:

| Metric | Description |
|---|---|
| `receiver_accepted_metric_points` | Data points successfully passed to consumer |
| `receiver_refused_metric_points` | Data points refused by downstream consumer |
| `receiver_failed_metric_points` | Data points that failed to parse/convert |
| `receiver_accepted_log_records` | Trap log records passed to consumer |
| `receiver_refused_log_records` | Trap log records refused by consumer |
| `receiver_failed_log_records` | Trap log records that failed to parse |
| `receiver_requests` | Request count with `outcome` attribute (success/refused/failure) |

### Custom SNMP-specific metrics

Defined in `metadata.yaml`, generated via `mdatagen`, using OTel Go SDK.

#### Polling health

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `snmpreceiver.targets` | UpDownCounter | `state` (active, errored, unreachable) | Number of targets by state |
| `snmpreceiver.poll.duration` | Histogram | `target` | Time to complete a full poll cycle |
| `snmpreceiver.poll.errors` | Counter | `target`, `error_type` (timeout, walk_fail, get_fail, parse) | Poll errors by type |
| `snmpreceiver.walks` | Counter | `target`, `metric_group`, `status` (success, error) | SNMP WALK operations |
| `snmpreceiver.gets` | Counter | `target`, `metric_group`, `status` (success, error) | SNMP GET operations |

#### Trap health

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `snmpreceiver.traps.received` | Counter | `version` (v1, v2c, v3) | Total traps received by version |
| `snmpreceiver.traps.rejected` | Counter | `reason` (auth_failed, parse_error, unknown_version) | Rejected traps by reason |

#### Connection health

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `snmpreceiver.connections` | UpDownCounter | `target`, `state` (connected, disconnected) | Connection state per target |

#### Consumer health

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `snmpreceiver.consumer.errors` | Counter | `signal` (metrics, logs), `error_type` (permanent, retryable) | Consumer errors by type |

## Testing Strategy

### Unit tests (per internal package)

**`internal/connection/`**
- Mock gosnmp responses for GET, WALK, BULKWALK
- v2c and v3 connection parameter setup
- Timeout and retry behavior
- Connection interface contract (enables mock usage upstream)

**`internal/poller/`**
- Poll scheduler starts/stops goroutines correctly
- Per-target collection with mock Connection
- Metric group collection: scalar GET, table WALK, mixed groups
- Index extraction from OID suffixes (simple and compound indexes)
- Lookup resolution (raw index -> human-readable label)
- `scalar_attributes` flow to resource attributes
- Error cases: target unreachable, partial walk failure, single OID timeout
- Concurrent targets don't interfere

**`internal/trapper/`**
- UDP listener starts and accepts connections
- v2c trap parsing with varbinds
- v3 trap parsing with auth validation
- v1-to-v2 normalization (RFC 2576)
- Auth rejection (wrong community, unknown v3 user)
- Malformed PDU handling

**`internal/metrics/`**
- pmetric construction for each metric type (counter -> monotonic Sum, gauge -> Gauge, up_down_counter -> non-monotonic Sum)
- Resource attributes (host, port, scalar_attributes)
- Scope metadata (name, version)
- Data point attributes from table indexes and lookups
- Timestamp assignment
- Empty/nil value handling

**`internal/logs/`**
- plog construction from trap PDUs
- Severity mapping for well-known trap OIDs
- Varbind-to-attribute conversion per SNMP type
- Resource attributes (source IP/port)
- Body format

### Integration tests (top-level package)

**`config_test.go`**
- Default config validation
- All validation rules (missing fields, invalid values)
- Auth block validation (v2c needs community, v3 needs username+auth)
- Metric group validation (metric needs name, type, oid)
- Config with only polling, only traps, both
- YAML deserialization with testdata files

**`factory_test.go`**
- Factory creates metrics receiver
- Factory creates logs receiver
- Shared instance pattern (metrics + logs get same receiver)

**`receiver_test.go`**
- Full lifecycle: Start -> poll -> collect -> Shutdown
- Metrics-only mode (no trap listener starts)
- Logs-only mode (no poller starts)
- Both modes simultaneously
- Graceful shutdown waits for in-flight polls
- Consumer error propagation

### Test infrastructure

**Mock SNMP agent:** A lightweight SNMP responder that serves configured OID values. Used across poller and integration tests. Lives in `internal/connection/` as a test helper.

**Test fixtures:** `testdata/` directory with sample config YAMLs (valid and invalid) and expected pmetric/plog outputs for golden-file testing.

## Implementation Phases

### Phase 1: Scaffolding
- `internal/connection/` -- Connection interface + gosnmp wrapper + mock
- Config structs, validation, factory, shared receiver pattern
- `metadata.yaml` with component metadata and custom telemetry definitions
- Receiver orchestrator skeleton (Start/Shutdown with no-op poller/trapper)
- Config tests, factory tests

### Phase 2: Polling
- `internal/poller/` -- Poll scheduler + collector
- `internal/metrics/` -- pmetric builder
- SNMP GET for scalars, WALK/BULKWALK for tables
- Index extraction, lookup resolution, scalar_attributes
- All polling unit tests + integration tests
- Custom polling observability metrics

### Phase 3: Trap Receiver
- `internal/trapper/` -- UDP trap listener
- `internal/logs/` -- plog builder
- v1-to-v2 normalization, auth validation
- All trap unit tests + integration tests
- Custom trap observability metrics

## Prior Art

| Tool | Key patterns borrowed |
|---|---|
| **Telegraf SNMP plugin** | `Connection` interface for testability, Table/Field model, index extraction via OID suffix stripping, `inherit_tags` for scalar->table attribute flow, separate poll vs trap components |
| **Prometheus SNMP Exporter** | Named auth blocks, lookup chains for index-to-label resolution, module/profile concept for reusable collection configs |
| **OTel OTLP Receiver** | `internal/` package structure, shared receiver pattern, ObsReport usage, mdatagen for telemetry |
| **OTel NATS JetStream Receiver** | Grafts-specific patterns: single-module structure, factory with receiverStore, receiverWrapper with sync.Once |

## Future Considerations

These are explicitly out of scope for the initial implementation but the design accommodates them:

- **MIB-based generator tool:** Offline tool that reads MIBs and produces metric_group configs with resolved OIDs, types, and descriptions. Config structure already supports this output format.
- **Dynamic target discovery:** Targets from a service discovery mechanism instead of static config.
- **Trap-to-metric correlation:** Correlating trap source IPs with polling targets to enrich trap logs with polled resource attributes.
