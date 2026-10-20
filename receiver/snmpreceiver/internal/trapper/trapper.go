// Package trapper implements an SNMP trap listener that converts incoming traps
// to OpenTelemetry logs via the consumer.Logs interface.
package trapper

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
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

const (
	snmpTrapOIDMIB = "1.3.6.1.6.3.1.1.4.1.0"
	// maxMessageSize is the largest UDP payload.
	maxMessageSize = 65535
)

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

// Run listens for traps until ctx is cancelled. It reads the socket itself
// rather than using gosnmp's TrapListener so that malformed packets cannot
// crash the process and informs are acknowledged only once authorized.
func (tr *Trapper) Run(ctx context.Context) {
	udpAddr, err := net.ResolveUDPAddr("udp", tr.listenAddr)
	if err != nil {
		tr.logger.Error("invalid SNMP trap listen address", zap.Error(err))
		return
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		tr.logger.Error("failed to start SNMP trap listener", zap.Error(err))
		return
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stop()
		_ = conn.Close()
	}()

	tr.mu.Lock()
	tr.resolvedAddr = conn.LocalAddr().String()
	tr.mu.Unlock()
	tr.logger.Info("SNMP trap listener ready", zap.String("address", tr.ListenAddr()))

	buf := make([]byte, maxMessageSize)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			tr.logger.Warn("failed to read SNMP trap", zap.Error(err))
			continue
		}
		tr.receive(conn, bytes.Clone(buf[:n]), remote)
	}
}

// receive authenticates one datagram and, when accepted, consumes it and
// acknowledges it if it is an inform.
func (tr *Trapper) receive(conn *net.UDPConn, msg []byte, addr *net.UDPAddr) {
	packet, err := tr.unmarshal(msg)
	if err != nil {
		tr.logger.Debug("dropped undecodable or unauthenticated trap",
			zap.String("addr", addr.String()), zap.Error(err))
		return
	}
	// gosnmp decodes a copy of SNMPv3 messages, so msg is still the raw packet.
	encrypted := packet.MsgFlags&gosnmp.AuthPriv == gosnmp.AuthPriv && scopedPDUEncrypted(msg)
	if !tr.isAuthorized(packet, encrypted) {
		tr.logger.Warn("received trap from unauthorized source",
			zap.String("addr", addr.String()))
		return
	}

	tr.handleTrap(packet, addr)

	if packet.PDUType == gosnmp.InformRequest {
		packet.PDUType = gosnmp.GetResponse
		packet.Error = gosnmp.NoError
		packet.ErrorIndex = 0
		out, err := packet.MarshalMsg()
		if err == nil {
			_, err = conn.WriteToUDP(out, addr)
		}
		if err != nil {
			tr.logger.Warn("failed to acknowledge SNMP inform", zap.String("addr", addr.String()), zap.Error(err))
		}
	}
}

// unmarshal decodes and, for SNMPv3, authenticates and decrypts a trap.
// gosnmp can panic on crafted input, so a panic is reported as an error.
func (tr *Trapper) unmarshal(msg []byte) (packet *gosnmp.SnmpPacket, err error) {
	defer func() {
		if r := recover(); r != nil {
			packet, err = nil, fmt.Errorf("malformed SNMP message: %v", r)
		}
	}()
	return tr.params.UnmarshalTrap(msg, false)
}

// scopedPDUEncrypted reports whether an SNMPv3 message carries its scoped PDU
// as an encrypted OCTET STRING rather than a plaintext SEQUENCE. gosnmp
// accepts a plaintext scoped PDU even when the message claims privacy.
func scopedPDUEncrypted(msg []byte) bool {
	_, rest, _, ok := berElement(msg)
	// Skip msgVersion, msgGlobalData and msgSecurityParameters.
	for range 3 {
		if !ok {
			return false
		}
		_, _, rest, ok = berElement(rest)
	}
	tag, _, _, ok := berElement(rest)
	return ok && tag == byte(gosnmp.OctetString)
}

// berElement splits the first definite-length BER element off b. It accepts
// non-minimal length encodings, as gosnmp does.
func berElement(b []byte) (tag byte, content, rest []byte, ok bool) {
	if len(b) < 2 {
		return 0, nil, nil, false
	}
	length, start := int(b[1]), 2
	if length&0x80 != 0 {
		n := length & 0x7f
		if n == 0 || n > 4 || len(b) < start+n {
			return 0, nil, nil, false
		}
		length = 0
		for _, c := range b[start : start+n] {
			length = length<<8 | int(c)
		}
		start += n
	}
	if len(b)-start < length {
		return 0, nil, nil, false
	}
	return b[0], b[start : start+length], b[start+length:], true
}

// handleTrap converts an authorized trap to a log record and consumes it.
func (tr *Trapper) handleTrap(packet *gosnmp.SnmpPacket, addr *net.UDPAddr) {
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
// user and verified the packet at the security level it claims. The remaining
// checks close gaps gosnmp leaves: a non-USM security model, a plaintext
// scoped PDU under the privacy flag, and a level below the user's.
func (tr *Trapper) isAuthorized(packet *gosnmp.SnmpPacket, encrypted bool) bool {
	if packet.Version == gosnmp.Version3 {
		// gosnmp skips authentication for any other security model.
		if packet.SecurityModel != gosnmp.UserSecurityModel {
			return false
		}
		if packet.MsgFlags&gosnmp.AuthPriv == gosnmp.AuthPriv && !encrypted {
			return false
		}
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
