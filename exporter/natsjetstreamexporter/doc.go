// Package natsjetstreamexporter implements an exporter that publishes
// telemetry data (traces, metrics, logs) to NATS JetStream streams.
//
// This exporter serializes telemetry data to OTLP protobuf format and
// publishes it to configurable JetStream subjects. It supports both
// synchronous and asynchronous publishing modes for different throughput
// and reliability trade-offs.
//
// The exporter is designed to be compatible with the natsjetstreamreceiver,
// using the same message format and default subject patterns for seamless
// integration.
package natsjetstreamexporter // import "go.olly.garden/grafts/exporter/natsjetstreamexporter"
