// Package natsjetstreamreceiver implements a receiver that consumes
// telemetry data (traces, metrics, logs) from NATS JetStream streams.
//
// This receiver uses pull-based consumption with JetStream consumers to provide
// backpressure control and reliable message delivery. It supports horizontal
// scaling through queue groups and handles OTLP protobuf formatted messages.
//
// The receiver creates one JetStream consumer per signal type (traces, metrics, logs)
// with signal-specific subject filters, enabling efficient message routing without
// application-level routing logic.
package natsjetstreamreceiver // import "go.olly.garden/grafts/receiver/natsjetstreamreceiver"
