# NATS JetStream Exporter

The NATS JetStream exporter publishes telemetry data (traces, metrics, logs) to NATS JetStream streams using OTLP protobuf format.

## Features

- Multi-signal support: traces, metrics, and logs in a single exporter instance
- Synchronous and asynchronous publishing modes
- OTLP protobuf format (compatible with natsjetstreamreceiver)
- Automatic reconnection with configurable backoff
- Graceful shutdown with pending message flush

## Configuration

### Basic Configuration

```yaml
exporters:
  natsjetstream:
    url: "nats://localhost:4222"
    stream: "telemetry"

service:
  pipelines:
    traces:
      exporters: [natsjetstream]
    metrics:
      exporters: [natsjetstream]
    logs:
      exporters: [natsjetstream]
```

### Full Configuration

```yaml
exporters:
  natsjetstream:
    # Connection settings
    url: "nats://localhost:4222"
    credentials_file: ""           # Optional NATS credentials

    # JetStream settings
    domain: ""                     # JetStream domain (for clustered deployments)
    stream: "telemetry"            # JetStream stream name (must exist)

    # Subject patterns for each signal type
    subjects:
      traces: "otlp.proto.traces"
      metrics: "otlp.proto.metrics"
      logs: "otlp.proto.logs"

    # Publishing settings
    publish_async: true            # Use async publishing for higher throughput
    flush_timeout: 5s              # Timeout for flushing pending publishes on shutdown

    # Connection resilience settings
    reconnect_wait: 2s
    max_reconnects: -1             # Infinite reconnects
    ping_interval: 2m

service:
  pipelines:
    traces:
      exporters: [natsjetstream]
    metrics:
      exporters: [natsjetstream]
    logs:
      exporters: [natsjetstream]
```

## Publishing Modes

### Async Publishing (Default)

Higher throughput, messages are batched and acknowledged asynchronously:
```yaml
exporters:
  natsjetstream:
    publish_async: true
```

### Sync Publishing

Lower throughput but immediate confirmation of each message:
```yaml
exporters:
  natsjetstream:
    publish_async: false
```

## Error Handling

- **Serialization errors**: Permanent failure (invalid data won't change)
- **Connection errors**: Retryable with automatic reconnection
- **Stream not found**: Permanent failure (configuration error)
- **Stream not available**: Retryable (temporary JetStream issue)

## Receiver Compatibility

This exporter is designed to work seamlessly with the [natsjetstreamreceiver](../../receiver/natsjetstreamreceiver/):
- Same default subject patterns
- Same OTLP protobuf message format
- Compatible JetStream configuration

## Example Configurations

### High-Throughput Mode

```yaml
exporters:
  natsjetstream:
    url: "nats://nats:4222"
    stream: "telemetry"
    publish_async: true
    flush_timeout: 10s             # More time for large backlogs

service:
  pipelines:
    traces:
      exporters: [natsjetstream]
```

## Requirements

- NATS Server with JetStream enabled
- JetStream stream must exist before starting the exporter
- Stream must be configured to accept the subject patterns used

## Status

- **Stability Level**: Alpha
- **Supported Signals**: Traces, Metrics, Logs
