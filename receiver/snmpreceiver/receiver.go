package snmpreceiver

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

// snmpReceiver implements a receiver that polls SNMP targets for metrics
// and listens for SNMP traps as logs.
type snmpReceiver struct {
	config      *Config
	settings    *receiver.Settings
	nextMetrics consumer.Metrics
	nextLogs    consumer.Logs
	obsrecv     *receiverhelper.ObsReport
	cancel      context.CancelFunc
	shutdownWG  sync.WaitGroup
}

// newSNMPReceiver creates a new SNMP receiver.
func newSNMPReceiver(cfg *Config, settings *receiver.Settings) (*snmpReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "snmp",
		ReceiverCreateSettings: *settings,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create obsreport: %w", err)
	}

	return &snmpReceiver{
		config:   cfg,
		settings: settings,
		obsrecv:  obsrecv,
	}, nil
}

// registerMetricsConsumer registers a metrics consumer.
func (r *snmpReceiver) registerMetricsConsumer(mc consumer.Metrics) {
	r.nextMetrics = mc
}

// registerLogsConsumer registers a logs consumer.
func (r *snmpReceiver) registerLogsConsumer(lc consumer.Logs) {
	r.nextLogs = lc
}

// Start begins the SNMP receiver.
func (r *snmpReceiver) Start(_ context.Context, _ component.Host) error {
	_, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.settings.Logger.Info("SNMP receiver started",
		zap.Int("targets", len(r.config.Targets)),
		zap.Bool("trap_listener", r.config.TrapListener != nil),
		zap.Bool("metrics", r.nextMetrics != nil),
		zap.Bool("logs", r.nextLogs != nil))

	return nil
}

// Shutdown gracefully shuts down the receiver.
func (r *snmpReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	r.shutdownWG.Wait()

	r.settings.Logger.Info("SNMP receiver shutdown complete")
	return nil
}
