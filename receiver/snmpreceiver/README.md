# SNMP Receiver

The SNMP receiver polls network devices for metrics via SNMP GET/WALK and listens for SNMP traps/informs as logs.

## Features

- **Polling**: Periodically collects metrics from SNMP targets using GET (scalars) and WALK/BULKWALK (tables)
- **Trap listening**: Receives SNMP traps and informs on a UDP listener, converts them to OpenTelemetry log records
- **SNMPv2c and SNMPv3**: Community-string and USM authentication (MD5, SHA, SHA-256/384/512, DES, AES, AES-192/256)
- **Metric groups**: Named, reusable collection profiles that define which OIDs to poll and how to map them to OTel metrics
- **Table-aware collection**: SNMP table walks with automatic index extraction, attribute enrichment, and lookup chains for index-to-label resolution
- **Named auth configs**: Define authentication credentials once, reference them across multiple targets
- **Concurrent polling**: One goroutine per target for parallel collection
- **Built-in observability**: Receiver metrics via `receiverhelper.ObsReport`

## Configuration

### Minimal Configuration (Polling Only)

```yaml
receivers:
  snmp:
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
            unit: "cs"

service:
  pipelines:
    metrics:
      receivers: [snmp]
```

### Full Configuration

```yaml
receivers:
  snmp:
    # Polling interval for all targets (default: 60s)
    collection_interval: 60s

    # SNMP request timeout per target (default: 5s)
    timeout: 5s

    # Number of SNMP request retries (default: 2)
    retries: 2

    # GETBULK max-repetitions for v2c/v3 table walks (default: 25)
    max_repetitions: 25

    # Named authentication configurations
    auth:
      public_v2c:
        version: v2c
        community: public

      secure_v3:
        version: v3
        username: monitor
        auth_protocol: SHA256              # MD5, SHA, SHA256, SHA384, SHA512
        auth_passphrase: ${env:SNMP_AUTH}
        privacy_protocol: AES256           # DES, AES, AES192, AES256
        privacy_passphrase: ${env:SNMP_PRIV}

    # SNMP targets to poll
    targets:
      - host: 192.168.1.1
        port: 161                          # default: 161
        auth: public_v2c
        metric_groups: [if_traffic, system]

      - host: 10.0.0.1
        auth: secure_v3
        metric_groups: [if_traffic]

    # Named metric groups (reusable collection profiles)
    metric_groups:
      if_traffic:
        walk: "1.3.6.1.2.1.2.2.1"         # ifTable subtree to WALK
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
          - source_indexes: [if_traffic_index]
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

    # Trap/inform listener (optional, produces logs)
    trap_listener:
      listen_address: 0.0.0.0:162
      accepted_auth: [public_v2c, secure_v3]

service:
  pipelines:
    metrics:
      receivers: [snmp]
    logs:
      receivers: [snmp]
```

## Concepts

### Metric Groups

A metric group is a named collection of OIDs that are polled together. Groups come in two flavors:

**Scalar groups** (no `walk` field): Each metric OID is fetched with SNMP GET. Use for single-value OIDs like `sysUpTime.0`.

**Table groups** (with `walk` field): The receiver walks the OID subtree and extracts table rows by index. Each row becomes a separate data point with index-based attributes. Use for tabular data like interface counters.

### Metric Types

| Config `type` | OpenTelemetry Type | Monotonic | Use for |
|---|---|---|---|
| `counter` | Sum | yes | Monotonically increasing values (e.g., byte counters, packet counts) |
| `gauge` | Gauge | n/a | Point-in-time values (e.g., CPU %, temperature, uptime) |
| `up_down_counter` | Sum | no | Values that can increase and decrease (e.g., active connections) |

### Table Attributes

When collecting table-based metrics, the receiver automatically creates attributes from the table index:

- **Index attribute**: Named `<metric_group_name>_index` (e.g., `if_traffic_index`), contains the raw SNMP table index.
- **Table attributes** (`attributes`): Other OID columns in the same table whose values become metric attributes per row.
- **Lookups** (`lookups`): Resolve raw indexes to human-readable labels by walking a separate OID table. For example, resolving interface index `1` to interface name `eth0`.

### Scalar Attributes

OIDs listed under `scalar_attributes` in a metric group are fetched once at startup and attached as **resource attributes** to all metrics from the target. Use for device-level metadata like `sysName`.

### Authentication

Auth configs are named and reusable. Each target references an auth config by name.

**SNMPv2c** requires `community`. **SNMPv3** requires `username` and supports optional auth/privacy:

| Security Level | Fields Required |
|---|---|
| `NoAuthNoPriv` | `username` only |
| `AuthNoPriv` | `username` + `auth_protocol` + `auth_passphrase` |
| `AuthPriv` | All of the above + `privacy_protocol` + `privacy_passphrase` |

The security level is determined automatically from the configured protocols.

### Trap Listener

When `trap_listener` is configured and the receiver is included in a logs pipeline, it listens for incoming SNMP traps on a UDP port. Each trap becomes a log record with:

- **Severity**: Mapped from well-known trap OIDs (`coldStart` = Info, `linkDown` = Warn, `authenticationFailure` = Error). Unknown traps default to Warn.
- **Body**: `"SNMP Trap: <trap OID>"`
- **Attributes**: `snmp.trap.oid`, `snmp.trap.type` (if well-known), `snmp.trap.version`, `snmp.trap.community`, `snmp.trap.uptime`, plus all varbinds as `snmp.varbind.<oid>`.
- **Resource**: `snmp.host` and `snmp.port` from the trap sender's IP/port.

SNMPv1 traps are accepted and normalized to v2c format.

## Example Configurations

### Interface Monitoring

Monitor interface traffic counters with human-readable interface names:

```yaml
receivers:
  snmp:
    auth:
      network:
        version: v2c
        community: public
    targets:
      - host: switch-01.example.com
        auth: network
        metric_groups: [interfaces]
    metric_groups:
      interfaces:
        walk: "1.3.6.1.2.1.2.2.1"
        metrics:
          - oid: "1.3.6.1.2.1.2.2.1.10"
            metric_name: snmp.interface.in_octets
            type: counter
            unit: "By"
          - oid: "1.3.6.1.2.1.2.2.1.16"
            metric_name: snmp.interface.out_octets
            type: counter
            unit: "By"
          - oid: "1.3.6.1.2.1.2.2.1.14"
            metric_name: snmp.interface.in_errors
            type: counter
          - oid: "1.3.6.1.2.1.2.2.1.20"
            metric_name: snmp.interface.out_errors
            type: counter
        lookups:
          - source_indexes: [interfaces_index]
            lookup_oid: "1.3.6.1.2.1.31.1.1.1.1"
            target_label: interface_name
```

### Trap-Only Receiver

Listen for SNMP traps without polling any targets:

```yaml
receivers:
  snmp:
    auth:
      traps_v2c:
        version: v2c
        community: public
    trap_listener:
      listen_address: 0.0.0.0:162
      accepted_auth: [traps_v2c]

service:
  pipelines:
    logs:
      receivers: [snmp]
```

### Multiple Device Types

Use the same metric groups across different device types:

```yaml
receivers:
  snmp:
    auth:
      switches:
        version: v2c
        community: switch-community
      servers:
        version: v3
        username: monitor
        auth_protocol: SHA256
        auth_passphrase: ${env:SNMP_AUTH}
    targets:
      - host: switch-01.example.com
        auth: switches
        metric_groups: [system, interfaces]
      - host: switch-02.example.com
        auth: switches
        metric_groups: [system, interfaces]
      - host: server-01.example.com
        auth: servers
        metric_groups: [system, host_resources]
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
        lookups:
          - source_indexes: [interfaces_index]
            lookup_oid: "1.3.6.1.2.1.31.1.1.1.1"
            target_label: interface_name
      host_resources:
        walk: "1.3.6.1.2.1.25.3.3.1"
        metrics:
          - oid: "1.3.6.1.2.1.25.3.3.1.2"
            metric_name: snmp.cpu.load
            type: gauge
            unit: "%"
```

## Data Model

### Polled Metrics

Each target produces a `ResourceMetrics` with:
- Resource attributes: `snmp.host`, `snmp.port`, plus any `scalar_attributes`
- Scope: `go.olly.garden/grafts/receiver/snmpreceiver`
- Metrics with data points per table row (for table groups) or a single data point (for scalar groups)

### Trap Logs

Each trap produces a `LogRecord` with:
- Resource attributes: `snmp.host` (sender IP), `snmp.port` (sender port)
- Scope: `go.olly.garden/grafts/receiver/snmpreceiver`
- Severity based on trap OID
- Trap metadata and varbinds as log attributes

## Error Handling

| Scenario | Behavior |
|---|---|
| Target unreachable on poll | Log warning, skip this cycle, retry next interval |
| Single OID walk fails | Log error, continue with remaining metric groups |
| Trap auth rejected | Log warning with source IP, drop the trap |
| Trap parse failure | Log error, drop the trap |
| Consumer error | Log warning, data is lost (SNMP is best-effort) |

## Requirements

- SNMP-enabled network devices accessible from the collector
- For SNMPv3: USM credentials configured on both the device and the receiver
- For traps: UDP port accessible from trap-sending devices (often port 162)

## Status

- **Stability Level**: Alpha
- **Supported Signals**: Metrics (polling), Logs (traps)
