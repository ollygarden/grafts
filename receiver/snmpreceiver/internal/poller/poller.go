package poller

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/metrics"
)

// TargetDef defines a target with its connection and metric groups.
type TargetDef struct {
	Host          string
	Port          int
	Conn          connection.Connection
	MetricGroups  []MetricGroupDef
	ResourceAttrs map[string]string
}

// Poller runs periodic SNMP collection for a set of targets.
type Poller struct {
	logger   *zap.Logger
	targets  []TargetDef
	interval time.Duration
	consumer consumer.Metrics
	obsrecv  *receiverhelper.ObsReport
}

// New creates a new Poller. obsrecv records receiver-level throughput and
// refusal metrics around the consume boundary.
func New(logger *zap.Logger, targets []TargetDef, interval time.Duration, consumer consumer.Metrics, obsrecv *receiverhelper.ObsReport) *Poller {
	return &Poller{
		logger:   logger,
		targets:  targets,
		interval: interval,
		consumer: consumer,
		obsrecv:  obsrecv,
	}
}

// Run starts polling. Blocks until ctx is cancelled.
// Spawns one goroutine per target. Each goroutine does an immediate first poll,
// then polls on the ticker interval, and returns when ctx is done.
func (p *Poller) Run(ctx context.Context) {
	if len(p.targets) == 0 {
		return
	}

	done := make(chan struct{})
	remaining := len(p.targets)

	for i := range p.targets {
		go func(target *TargetDef) {
			defer func() {
				done <- struct{}{}
			}()
			p.pollTarget(ctx, target)
		}(&p.targets[i])
	}

	for i := 0; i < remaining; i++ {
		<-done
	}
}

// pollTarget runs the poll loop for one target.
func (p *Poller) pollTarget(ctx context.Context, target *TargetDef) {
	// Immediate first poll.
	p.collectAndSend(ctx, target)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.collectAndSend(ctx, target)
		}
	}
}

// collectAndSend collects all metric groups for the target and sends to consumer.
func (p *Poller) collectAndSend(ctx context.Context, target *TargetDef) {
	var allCollected []metrics.CollectedMetric

	for _, group := range target.MetricGroups {
		collected, err := Collect(target.Conn, group)
		if err != nil {
			p.logger.Warn("Failed to collect metric group",
				zap.String("target", target.Host),
				zap.String("group", group.Name),
				zap.Error(err))
			continue
		}
		allCollected = append(allCollected, collected...)
	}

	if len(allCollected) == 0 {
		return
	}

	md := metrics.BuildMetrics(target.Host, target.Port, target.ResourceAttrs, allCollected)
	obsCtx := p.obsrecv.StartMetricsOp(ctx)
	err := p.consumer.ConsumeMetrics(obsCtx, md)
	p.obsrecv.EndMetricsOp(obsCtx, "snmp", md.DataPointCount(), err)
	if err != nil {
		p.logger.Warn("Failed to consume metrics",
			zap.String("target", target.Host),
			zap.Error(err))
	}
}
