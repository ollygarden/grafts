// Package trapper implements an SNMP trap listener that converts incoming traps
// to OpenTelemetry logs via the consumer.Logs interface.
package trapper

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/logs"
)

const snmpTrapOIDMIB = "1.3.6.1.6.3.1.1.4.1.0"

// AuthEntry holds authentication credentials for a single auth profile.
type AuthEntry struct {
	Version   string // "v2c" or "v3"
	Community string
	Username  string
}

// Trapper listens for incoming SNMP traps on a UDP address and forwards them
// as OpenTelemetry logs to the configured consumer.
type Trapper struct {
	logger       *zap.Logger
	listenAddr   string
	acceptedAuth []AuthEntry
	consumer     consumer.Logs
	obsrecv      *receiverhelper.ObsReport
	mu           sync.Mutex
	resolvedAddr string
}

// New creates a new Trapper. obsrecv records receiver-level throughput and
// refusal metrics around the consume boundary.
func New(logger *zap.Logger, listenAddr string, acceptedAuth []AuthEntry, consumer consumer.Logs, obsrecv *receiverhelper.ObsReport) *Trapper {
	return &Trapper{
		logger:       logger,
		listenAddr:   listenAddr,
		acceptedAuth: acceptedAuth,
		consumer:     consumer,
		obsrecv:      obsrecv,
	}
}

// ListenAddr returns the resolved listen address, available after Run starts.
func (tr *Trapper) ListenAddr() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.resolvedAddr
}

// Run starts the trap listener and blocks until ctx is cancelled.
func (tr *Trapper) Run(ctx context.Context) {
	listener := gosnmp.NewTrapListener()
	listener.OnNewTrap = tr.handleTrap
	listener.Params = gosnmp.Default

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := listener.Listen(tr.listenAddr); err != nil {
			select {
			case <-ctx.Done():
				// expected shutdown
			default:
				tr.logger.Error("trap listener exited unexpectedly", zap.Error(err))
			}
		}
	}()

	// Wait for the listener to be ready
	select {
	case <-listener.Listening():
		tr.mu.Lock()
		tr.resolvedAddr = tr.listenAddr
		tr.mu.Unlock()
		tr.logger.Info("SNMP trap listener ready", zap.String("address", tr.listenAddr))
	case <-done:
		return
	}

	select {
	case <-ctx.Done():
		listener.Close()
	case <-done:
	}
}

// handleTrap processes an incoming SNMP trap packet.
func (tr *Trapper) handleTrap(packet *gosnmp.SnmpPacket, addr *net.UDPAddr) {
	if !tr.isAuthorized(packet) {
		tr.logger.Warn("received trap from unauthorized source",
			zap.String("addr", addr.String()))
		return
	}

	var trapOID string
	var uptime int64
	varbinds := make(map[string]interface{})

	for _, v := range packet.Variables {
		// gosnmp may prefix OID names with a leading "."
		name := strings.TrimPrefix(v.Name, ".")
		switch name {
		case snmpTrapOIDMIB:
			if oid, ok := v.Value.(string); ok {
				trapOID = strings.TrimPrefix(oid, ".")
			}
		case "1.3.6.1.2.1.1.3.0":
			if val, ok := v.Value.(uint32); ok {
				uptime = int64(val)
			}
		default:
			varbinds[name] = v.Value
		}
	}

	version := "v2c"
	switch packet.Version {
	case gosnmp.Version1:
		version = "v1"
	case gosnmp.Version2c:
		version = "v2c"
	case gosnmp.Version3:
		version = "v3"
	}

	trapData := logs.TrapData{
		SourceIP:   addr.IP.String(),
		SourcePort: addr.Port,
		Version:    version,
		Community:  packet.Community,
		TrapOID:    trapOID,
		Uptime:     uptime,
		Varbinds:   varbinds,
		Timestamp:  time.Now(),
	}

	pLogs := logs.BuildLog(trapData)
	ctx := tr.obsrecv.StartLogsOp(context.Background())
	err := tr.consumer.ConsumeLogs(ctx, pLogs)
	tr.obsrecv.EndLogsOp(ctx, "snmp", pLogs.LogRecordCount(), err)
	if err != nil {
		tr.logger.Error("failed to consume trap logs", zap.Error(err))
	}
}

// isAuthorized checks whether the packet matches any accepted auth entry.
func (tr *Trapper) isAuthorized(packet *gosnmp.SnmpPacket) bool {
	// If no auth entries are configured, accept all.
	if len(tr.acceptedAuth) == 0 {
		return true
	}

	for _, entry := range tr.acceptedAuth {
		switch entry.Version {
		case "v2c":
			if packet.Community == entry.Community {
				return true
			}
		case "v3":
			if usm, ok := packet.SecurityParameters.(*gosnmp.UsmSecurityParameters); ok {
				if usm.UserName == entry.Username {
					return true
				}
			}
		}
	}
	return false
}
