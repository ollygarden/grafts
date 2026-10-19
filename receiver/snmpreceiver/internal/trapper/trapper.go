// Package trapper implements an SNMP trap listener that converts incoming traps
// to OpenTelemetry logs via the consumer.Logs interface.
package trapper

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"go.olly.garden/grafts/receiver/snmpreceiver/internal/connection"
	"go.olly.garden/grafts/receiver/snmpreceiver/internal/logs"
)

const snmpTrapOIDMIB = "1.3.6.1.6.3.1.1.4.1.0"

// AuthEntry holds authentication credentials for a single auth profile.
// A "v2c" entry accepts SNMPv1 and SNMPv2c traps carrying its community; a
// "v3" entry accepts SNMPv3 traps from its USM user at its security level.
type AuthEntry struct {
	Version           string // "v2c" or "v3"
	Community         string
	Username          string
	AuthProtocol      string
	AuthPassphrase    string
	PrivacyProtocol   string
	PrivacyPassphrase string
}

// Trapper listens for incoming SNMP traps on a UDP address and forwards them
// as OpenTelemetry logs to the configured consumer.
type Trapper struct {
	logger       *zap.Logger
	listenAddr   string
	communities  [][]byte
	acceptAll    bool
	params       *gosnmp.GoSNMP
	consumer     consumer.Logs
	obsrecv      *receiverhelper.ObsReport
	mu           sync.Mutex
	resolvedAddr string
}

// New creates a new Trapper. obsrecv records receiver-level throughput and
// refusal metrics around the consume boundary. With no acceptedAuth entries,
// every SNMPv1/v2c trap is accepted; SNMPv3 traps always require a matching
// "v3" entry because gosnmp needs the user's keys to verify them.
func New(logger *zap.Logger, listenAddr string, acceptedAuth []AuthEntry, consumer consumer.Logs, obsrecv *receiverhelper.ObsReport) (*Trapper, error) {
	tr := &Trapper{
		logger:     logger,
		listenAddr: listenAddr,
		acceptAll:  len(acceptedAuth) == 0,
		consumer:   consumer,
		obsrecv:    obsrecv,
	}

	// gosnmp authenticates and decrypts SNMPv3 traps against this table; an
	// empty table makes it drop every SNMPv3 trap.
	usmTable := gosnmp.NewSnmpV3SecurityParametersTable(gosnmp.Logger{})
	for _, entry := range acceptedAuth {
		switch entry.Version {
		case "v2c":
			tr.communities = append(tr.communities, []byte(entry.Community))
		case "v3":
			usm, _ := connection.USM(entry.Username, entry.AuthProtocol, entry.AuthPassphrase, entry.PrivacyProtocol, entry.PrivacyPassphrase)
			if err := usmTable.Add(entry.Username, usm); err != nil {
				return nil, fmt.Errorf("SNMPv3 user %q: %w", entry.Username, err)
			}
		}
	}
	if tr.acceptAll {
		logger.Warn("SNMP trap listener has no accepted_auth: every SNMPv1/v2c trap is accepted regardless of community, and SNMPv3 traps are dropped; configure accepted_auth to authenticate traps")
	}
	tr.params = &gosnmp.GoSNMP{
		Version:                     gosnmp.Version3,
		TrapSecurityParametersTable: usmTable,
	}
	return tr, nil
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
	listener.Params = tr.params

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
// SNMPv3 packets reach this point only after gosnmp has matched a configured
// user and verified the packet at the security level it claims, so the
// remaining check is that the claimed level is not below that user's level.
func (tr *Trapper) isAuthorized(packet *gosnmp.SnmpPacket) bool {
	if packet.Version == gosnmp.Version3 {
		// gosnmp leaves the matched user's configured protocols on the packet.
		usm, ok := packet.SecurityParameters.(*gosnmp.UsmSecurityParameters)
		if !ok {
			return false
		}
		want := gosnmp.NoAuthNoPriv
		if usm.AuthenticationProtocol > gosnmp.NoAuth {
			want = gosnmp.AuthNoPriv
		}
		if usm.PrivacyProtocol > gosnmp.NoPriv {
			want = gosnmp.AuthPriv
		}
		// Compare bits rather than ordering: the invalid priv-without-auth
		// flag value must not satisfy an authNoPriv user.
		return packet.MsgFlags&want == want
	}

	if tr.acceptAll {
		return true
	}
	community := []byte(packet.Community)
	return slices.ContainsFunc(tr.communities, func(c []byte) bool {
		return subtle.ConstantTimeCompare(community, c) == 1
	})
}
