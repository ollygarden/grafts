package snmpreceiver

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

// componentType is the name of this receiver in configuration files.
const componentType = "snmp"

// receiverStore holds shared receiver instances keyed by component ID.
// This ensures a single receiver instance handles all signal types for a given configuration.
type receiverStore struct {
	mu        sync.Mutex
	receivers map[component.ID]*sharedReceiver
}

type sharedReceiver struct {
	receiver  *snmpReceiver
	id        component.ID
	startOnce sync.Once
	stopOnce  sync.Once
}

var store = &receiverStore{
	receivers: make(map[component.ID]*sharedReceiver),
}

// NewFactory creates a factory for the SNMP receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(componentType),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha),
	)
}

// getOrCreateReceiver gets or creates a shared receiver instance for the given ID.
func (s *receiverStore) getOrCreateReceiver(
	id component.ID,
	cfg *Config,
	settings *receiver.Settings,
) (*sharedReceiver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.receivers[id]; ok {
		return r, nil
	}

	recv, err := newSNMPReceiver(cfg, settings)
	if err != nil {
		return nil, err
	}

	shared := &sharedReceiver{
		receiver: recv,
		id:       id,
	}
	s.receivers[id] = shared
	return shared, nil
}

// remove removes a receiver instance from the store.
func (s *receiverStore) remove(id component.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.receivers, id)
}

// createMetricsReceiver creates a metrics receiver instance.
func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	oCfg := cfg.(*Config)
	shared, err := store.getOrCreateReceiver(settings.ID, oCfg, &settings)
	if err != nil {
		return nil, err
	}
	shared.receiver.registerMetricsConsumer(nextConsumer)
	return &receiverWrapper{shared: shared}, nil
}

// createLogsReceiver creates a logs receiver instance.
func createLogsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	oCfg := cfg.(*Config)
	shared, err := store.getOrCreateReceiver(settings.ID, oCfg, &settings)
	if err != nil {
		return nil, err
	}
	shared.receiver.registerLogsConsumer(nextConsumer)
	return &receiverWrapper{shared: shared}, nil
}

// receiverWrapper wraps a shared receiver and ensures Start/Shutdown are called only once.
type receiverWrapper struct {
	shared *sharedReceiver
}

func (w *receiverWrapper) Start(ctx context.Context, host component.Host) error {
	var err error
	w.shared.startOnce.Do(func() {
		err = w.shared.receiver.Start(ctx, host)
	})
	return err
}

func (w *receiverWrapper) Shutdown(ctx context.Context) error {
	var err error
	w.shared.stopOnce.Do(func() {
		err = w.shared.receiver.Shutdown(ctx)
		store.remove(w.shared.id)
	})
	return err
}
