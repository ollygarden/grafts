# NATS JetStream Receiver

The NATS JetStream receiver consumes telemetry data (traces, metrics, logs) from NATS JetStream streams using pull-based consumers.

## Features

- Multi-signal support: traces, metrics, and logs in a single receiver instance
- Pull-based consumption with JetStream consumers for backpressure control
- OTLP protobuf format support
- Horizontal scaling via queue groups (enabled by default)
- Reliable message delivery with ack/nak handling
- Graceful shutdown with message draining
- Built-in observability metrics via receiverhelper

## Configuration

### Basic Configuration

```yaml
receivers:
  natsjetstream:
    url: "nats://localhost:4222"
    stream: "telemetry"

service:
  pipelines:
    traces:
      receivers: [natsjetstream]
    metrics:
      receivers: [natsjetstream]
    logs:
      receivers: [natsjetstream]
```

### Full Configuration

```yaml
receivers:
  natsjetstream:
    # Connection settings
    url: "nats://localhost:4222"
    credentials_file: ""           # Optional NATS credentials

    # JetStream settings
    domain: ""                     # JetStream domain (required for clustered deployments)
    stream: "telemetry"            # JetStream stream name

    # Consumer settings
    consumer_name: "otel-receiver"           # Durable consumer name
    consumer_group: "natsjetstream-receiver" # Queue group (enabled by default)

    # Subject patterns (explicit config with sensible defaults)
    subjects:
      traces: "otlp.proto.traces"
      metrics: "otlp.proto.metrics"
      logs: "otlp.proto.logs"

    # Reliability settings
    ack_wait: 30s                  # Time before message redelivery
    max_deliver: 5                 # Max redelivery attempts
    max_ack_pending: 1000          # Max unacked messages

    # Connection resilience
    reconnect_wait: 2s
    max_reconnects: -1             # Infinite reconnects
    ping_interval: 2m
```

## Architecture

### Shared Receiver Pattern

The receiver uses a shared component pattern (similar to the OTLP receiver):
- Single receiver instance handles all signal types for a given configuration
- One JetStream consumer per signal type with signal-specific subject filters
- Efficient resource usage with a single NATS connection

### Message Processing

1. Pull-based consumption using `consumer.Messages()` iterator pattern
2. Explicit flow control via `iter.Next()`
3. Messages are expected in OTLP protobuf format
4. Automatic ack/nak handling based on consumer error responses
5. Graceful shutdown with `Drain()` to process in-flight messages

## Horizontal Scaling

Queue groups are enabled by default for horizontal scaling:
- Default consumer group: `natsjetstream-receiver`
- Multiple receiver instances automatically share the workload
- Each instance processes a subset of messages

## Error Handling

- **Parse errors**: Permanent failure - message is terminated (no retry, invalid data won't change)
- **Consumer errors**: Retryable errors trigger nak with delay, permanent errors terminate the message
- **Connection errors**: Automatic reconnection with exponential backoff

## Observability

The receiver automatically reports metrics via OpenTelemetry Collector's receiverhelper:
- `otelcol_receiver_accepted_spans`
- `otelcol_receiver_refused_spans`
- `otelcol_receiver_accepted_metric_points`
- `otelcol_receiver_refused_metric_points`
- `otelcol_receiver_accepted_log_records`
- `otelcol_receiver_refused_log_records`

## Example Configurations

### Split Streams (Multiple Instances)

```yaml
receivers:
  natsjetstream/traces:
    url: "nats://nats:4222"
    stream: "og-traces"
    consumer_name: "bamboo-traces"
    subjects:
      traces: "og.*.in.otlp.proto.traces"

  natsjetstream/metrics:
    url: "nats://nats:4222"
    stream: "og-metrics"
    consumer_name: "bamboo-metrics"
    subjects:
      metrics: "og.*.in.otlp.proto.metrics"

service:
  pipelines:
    traces:
      receivers: [natsjetstream/traces]
    metrics:
      receivers: [natsjetstream/metrics]
```

## Requirements

- NATS Server with JetStream enabled
- JetStream stream must exist before starting the receiver
- Messages must be in OTLP protobuf format

## Status

- **Stability Level**: Alpha
- **Supported Signals**: Traces, Metrics, Logs
