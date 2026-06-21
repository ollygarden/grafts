package natsjetstreamexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// componentType is the name of this exporter in configuration files.
const componentType = "natsjetstream"

// NewFactory creates a factory for the NATS JetStream exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(componentType),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelAlpha),
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	oCfg := cfg.(*Config)
	exp, err := newNatsJetStreamExporter(oCfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	oCfg := cfg.(*Config)
	exp, err := newNatsJetStreamExporter(oCfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.pushMetrics,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	oCfg := cfg.(*Config)
	exp, err := newNatsJetStreamExporter(oCfg, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.pushLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	)
}
