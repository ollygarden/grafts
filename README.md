# 🌱 Grafts

Custom OpenTelemetry Collector components for OllyGarden.

## Components

### Receivers

| Component | Status | Description |
|-----------|--------|-------------|
| [natsjetstreamreceiver](receiver/natsjetstreamreceiver/) | Alpha | Consumes traces, metrics, and logs from NATS JetStream streams |
| [snmpreceiver](receiver/snmpreceiver/) | Alpha | Polls SNMP targets for metrics and listens for traps as logs |

### Exporters

| Component | Status | Description |
|-----------|--------|-------------|
| [natsjetstreamexporter](exporter/natsjetstreamexporter/) | Alpha | Publishes traces, metrics, and logs to NATS JetStream streams |

## Building

A test distribution is available under `distributions/grafts/` for local development:

```bash
cd distributions/grafts
make build   # Build the collector
make run     # Run with sample config
```

For production use, see [OllyGarden Tulip](https://olly.garden/tulip).

## Questions and Answers

**Q: What's up with this name?**
A: In horticulture, grafting is the technique of attaching a shoot or bud from one plant onto another, allowing them to grow together as one. Similarly, Grafts contains custom components that are "grafted" onto the OpenTelemetry Collector to extend its capabilities.

## License

[Apache License 2.0](LICENSE)
