package snmpreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/poller"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/trapper"
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
	connections []connection.Connection
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
func (r *snmpReceiver) Start(ctx context.Context, _ component.Host) error {
	loopCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	if r.nextMetrics != nil && len(r.config.Targets) > 0 {
		targets, err := r.buildTargetDefs(ctx)
		if err != nil {
			// Close any connections opened before the failure.
			for _, conn := range r.connections {
				_ = conn.Close()
			}
			r.connections = nil
			cancel()
			return fmt.Errorf("failed to build target definitions: %w", err)
		}
		p := poller.New(r.settings.Logger, targets, r.config.CollectionInterval, r.nextMetrics)
		r.shutdownWG.Add(1)
		go func() {
			defer r.shutdownWG.Done()
			p.Run(loopCtx)
		}()
		r.settings.Logger.Info("SNMP polling started",
			zap.Int("targets", len(targets)),
			zap.Duration("interval", r.config.CollectionInterval))
	}

	if r.nextLogs != nil && r.config.TrapListener != nil {
		var authEntries []trapper.AuthEntry
		for _, authName := range r.config.TrapListener.AcceptedAuth {
			auth := r.config.Auth[authName]
			authEntries = append(authEntries, trapper.AuthEntry{
				Version:   auth.Version,
				Community: auth.Community,
				Username:  auth.Username,
			})
		}
		tr := trapper.New(r.settings.Logger, r.config.TrapListener.ListenAddress, authEntries, r.nextLogs)
		r.shutdownWG.Add(1)
		go func() {
			defer r.shutdownWG.Done()
			tr.Run(loopCtx)
		}()
		r.settings.Logger.Info("SNMP trap listener started",
			zap.String("address", r.config.TrapListener.ListenAddress))
	}

	return nil
}

// Shutdown gracefully shuts down the receiver.
func (r *snmpReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.shutdownWG.Wait()
	var closeErr error
	for _, conn := range r.connections {
		closeErr = errors.Join(closeErr, conn.Close())
	}
	r.settings.Logger.Info("SNMP receiver shutdown complete")
	return closeErr
}

// buildTargetDefs builds poller.TargetDef entries from config, establishing SNMP connections.
func (r *snmpReceiver) buildTargetDefs(ctx context.Context) ([]poller.TargetDef, error) {
	var targets []poller.TargetDef
	for _, tc := range r.config.Targets {
		auth := r.config.Auth[tc.Auth]
		port := tc.Port
		if port == 0 {
			port = 161
		}

		version := connection.V2c
		if auth.Version == "v3" {
			version = connection.V3
		}

		conn, err := connection.NewConnection(connection.Params{
			Host:              tc.Host,
			Port:              uint16(port),
			Version:           version,
			Community:         auth.Community,
			Username:          auth.Username,
			AuthProtocol:      auth.AuthProtocol,
			AuthPassphrase:    auth.AuthPassphrase,
			PrivacyProtocol:   auth.PrivacyProtocol,
			PrivacyPassphrase: auth.PrivacyPassphrase,
			Timeout:           r.config.Timeout,
			Retries:           r.config.Retries,
			MaxRepetitions:    uint32(r.config.MaxRepetitions),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s:%d: %w", tc.Host, port, err)
		}
		r.connections = append(r.connections, conn)

		var groups []poller.MetricGroupDef
		for _, mgName := range tc.MetricGroups {
			groups = append(groups, convertMetricGroup(mgName, r.config.MetricGroups[mgName]))
		}

		resourceAttrs := map[string]string{}
		for _, group := range groups {
			attrs, err := poller.CollectScalarAttributes(conn, group)
			if err != nil {
				r.settings.Logger.Warn("Failed to collect scalar attributes",
					zap.String("target", tc.Host),
					zap.Error(err))
			}
			for k, v := range attrs {
				resourceAttrs[k] = v
			}
		}

		targets = append(targets, poller.TargetDef{
			Host:          tc.Host,
			Port:          port,
			Conn:          conn,
			MetricGroups:  groups,
			ResourceAttrs: resourceAttrs,
		})
	}
	return targets, nil
}

// convertMetricGroup converts a MetricGroupConfig to a poller.MetricGroupDef.
func convertMetricGroup(name string, mg MetricGroupConfig) poller.MetricGroupDef {
	var metricDefs []poller.MetricDef
	for _, m := range mg.Metrics {
		metricDefs = append(metricDefs, poller.MetricDef{
			OID:         m.OID,
			MetricName:  m.MetricName,
			Type:        m.Type,
			Unit:        m.Unit,
			Description: m.Description,
		})
	}
	var attrDefs []poller.AttributeDef
	for _, a := range mg.Attributes {
		attrDefs = append(attrDefs, poller.AttributeDef{OID: a.OID, Name: a.Name})
	}
	var scalarAttrDefs []poller.AttributeDef
	for _, a := range mg.ScalarAttributes {
		scalarAttrDefs = append(scalarAttrDefs, poller.AttributeDef{OID: a.OID, Name: a.Name})
	}
	var lookupDefs []poller.LookupDef
	for _, l := range mg.Lookups {
		lookupDefs = append(lookupDefs, poller.LookupDef{
			SourceIndexes: l.SourceIndexes,
			LookupOID:     l.LookupOID,
			TargetLabel:   l.TargetLabel,
		})
	}
	return poller.MetricGroupDef{
		Name:             name,
		Walk:             mg.Walk,
		Metrics:          metricDefs,
		Attributes:       attrDefs,
		ScalarAttributes: scalarAttrDefs,
		Lookups:          lookupDefs,
	}
}
