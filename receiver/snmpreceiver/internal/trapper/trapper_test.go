package trapper

import (
	"bytes"
	"context"
	"encoding/asn1"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap/zaptest"
)

// newTestObsReport builds an ObsReport backed by Nop telemetry for tests.
func newTestObsReport(t *testing.T) *receiverhelper.ObsReport {
	t.Helper()
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.MustNewID("snmp"),
		Transport:              "snmp",
		ReceiverCreateSettings: receivertest.NewNopSettings(component.MustNewType("snmp")),
	})
	require.NoError(t, err)
	return obsrecv
}

const (
	coldStartOID = "1.3.6.1.6.3.1.1.5.1"
	linkDownOID  = "1.3.6.1.6.3.1.1.5.3"
	// senderEngineID is the authoritative engine ID of the trap sender, which
	// localizes the SNMPv3 keys.
	senderEngineID = "\x80\x00\x1f\x88\x80grafts"
)

// sender describes how a test trap is sent.
type sender struct {
	version   gosnmp.SnmpVersion
	community string
	flags     gosnmp.SnmpV3MsgFlags
	usm       *gosnmp.UsmSecurityParameters
}

func v2cSender(community string) sender {
	return sender{version: gosnmp.Version2c, community: community}
}

func v1Sender(community string) sender {
	return sender{version: gosnmp.Version1, community: community}
}

func v3Sender(user string, authProto gosnmp.SnmpV3AuthProtocol, authPass string, privProto gosnmp.SnmpV3PrivProtocol, privPass string) sender {
	flags := gosnmp.NoAuthNoPriv
	if authProto != gosnmp.NoAuth {
		flags = gosnmp.AuthNoPriv
	}
	if privProto != gosnmp.NoPriv {
		flags = gosnmp.AuthPriv
	}
	return sender{
		version: gosnmp.Version3,
		flags:   flags,
		usm: &gosnmp.UsmSecurityParameters{
			UserName:                 user,
			AuthenticationProtocol:   authProto,
			AuthenticationPassphrase: authPass,
			PrivacyProtocol:          privProto,
			PrivacyPassphrase:        privPass,
			AuthoritativeEngineID:    senderEngineID,
			AuthoritativeEngineBoots: 1,
			AuthoritativeEngineTime:  1,
		},
	}
}

// dial connects a gosnmp client for s to addr.
func dial(t *testing.T, addr string, s sender, timeout time.Duration) *gosnmp.GoSNMP {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)

	g := &gosnmp.GoSNMP{
		Target:    host,
		Port:      uint16(port),
		Version:   s.version,
		Community: s.community,
		Timeout:   timeout,
	}
	if s.version == gosnmp.Version3 {
		g.SecurityModel = gosnmp.UserSecurityModel
		g.MsgFlags = s.flags
		g.SecurityParameters = s.usm
	}
	require.NoError(t, g.Connect())
	t.Cleanup(func() { _ = g.Conn.Close() })
	return g
}

// sendTrap sends a trap carrying trapOID to addr over real UDP.
func sendTrap(t *testing.T, addr string, s sender, trapOID string) {
	t.Helper()
	g := dial(t, addr, s, time.Second)
	trap := gosnmp.SnmpTrap{
		Variables: []gosnmp.SnmpPDU{
			{Name: "1.3.6.1.2.1.1.3.0", Type: gosnmp.TimeTicks, Value: uint32(123456)},
			{Name: snmpTrapOIDMIB, Type: gosnmp.ObjectIdentifier, Value: trapOID},
		},
	}
	if s.version == gosnmp.Version1 {
		// SNMPv1 traps carry the trap identity in the PDU header, not a varbind.
		trap = gosnmp.SnmpTrap{
			Enterprise:   trapOID,
			AgentAddress: "127.0.0.1",
			GenericTrap:  6,
			Variables: []gosnmp.SnmpPDU{
				{Name: "1.3.6.1.2.1.1.5.0", Type: gosnmp.OctetString, Value: "agent"},
			},
		}
	}
	_, err := g.SendTrap(trap)
	require.NoError(t, err)
}

// captureTrap returns the bytes a sender puts on the wire for a coldStart trap.
func captureTrap(t *testing.T, s sender) []byte {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	sendTrap(t, conn.LocalAddr().String(), s, coldStartOID)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, maxMessageSize)
	n, _, err := conn.ReadFromUDP(buf)
	require.NoError(t, err)
	return buf[:n]
}

// sendInform sends a v2c inform and returns the error from waiting for its
// acknowledgement.
func sendInform(t *testing.T, addr, community string) error {
	t.Helper()
	_, err := dial(t, addr, v2cSender(community), 300*time.Millisecond).SendTrap(gosnmp.SnmpTrap{
		IsInform: true,
		Variables: []gosnmp.SnmpPDU{
			{Name: snmpTrapOIDMIB, Type: gosnmp.ObjectIdentifier, Value: coldStartOID},
		},
	})
	return err
}

func tlv(class, tag int, compound bool, children ...[]byte) []byte {
	b, err := asn1.Marshal(asn1.RawValue{Class: class, Tag: tag, IsCompound: compound, Bytes: bytes.Join(children, nil)})
	if err != nil {
		panic(err)
	}
	return b
}

func seq(children ...[]byte) []byte {
	return tlv(asn1.ClassUniversal, asn1.TagSequence, true, children...)
}

func octets(s string) []byte { return tlv(asn1.ClassUniversal, asn1.TagOctetString, false, []byte(s)) }

func integer(v int) []byte { return tlv(asn1.ClassUniversal, asn1.TagInteger, false, []byte{byte(v)}) }

func oid(o ...int) []byte {
	b, err := asn1.Marshal(asn1.ObjectIdentifier(o))
	if err != nil {
		panic(err)
	}
	return b
}

// rawV3Trap hand-encodes an SNMPv3 coldStart trap so tests can set header
// fields gosnmp's encoder never produces. Values passed to integer must fit
// in one positive byte.
func rawV3Trap(user string, flags byte, securityModel int, authParams string, withScopedPDU bool) []byte {
	usm := seq(octets(senderEngineID), integer(1), integer(1), octets(user), octets(authParams), octets(""))
	parts := [][]byte{
		integer(3),
		seq(integer(1), tlv(asn1.ClassUniversal, asn1.TagInteger, false, []byte{0x00, 0xff, 0xe3}), octets(string([]byte{flags})), integer(securityModel)),
		octets(string(usm)),
	}
	if withScopedPDU {
		varbinds := seq(
			seq(oid(1, 3, 6, 1, 2, 1, 1, 3, 0), tlv(asn1.ClassApplication, 3, false, []byte{1})),
			seq(oid(1, 3, 6, 1, 6, 3, 1, 1, 4, 1, 0), oid(1, 3, 6, 1, 6, 3, 1, 1, 5, 1)),
		)
		pdu := tlv(asn1.ClassContextSpecific, 7, true, integer(1), integer(0), integer(0), varbinds)
		parts = append(parts, seq(octets(senderEngineID), octets(""), pdu))
	}
	return seq(parts...)
}

// startTrapper runs a trapper on a free local UDP port until the test ends.
func startTrapper(t *testing.T, auth []AuthEntry) (string, *consumertest.LogsSink) {
	t.Helper()
	sink := &consumertest.LogsSink{}
	tr, err := New(zaptest.NewLogger(t), allocateUDPPort(t), auth, sink, newTestObsReport(t))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return waitForListenAddr(t, tr), sink
}

// firstRecordAttrs waits for a log record and returns its attributes.
func firstRecordAttrs(t *testing.T, sink *consumertest.LogsSink) pcommon.Map {
	t.Helper()
	require.Eventually(t, func() bool { return sink.LogRecordCount() > 0 }, 2*time.Second, 10*time.Millisecond)
	return sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
}

// receivedVersions sends a v2c sentinel trap after whatever the test sent and
// waits for it, so every earlier trap has been processed. It returns the
// snmp.trap.version of each record received before the sentinel.
func receivedVersions(t *testing.T, addr string, sink *consumertest.LogsSink, sentinel sender) []string {
	t.Helper()
	sendTrap(t, addr, sentinel, linkDownOID)

	var versions []string
	require.Eventually(t, func() bool {
		versions = versions[:0]
		for _, ld := range sink.AllLogs() {
			lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			if oid, _ := lr.Attributes().Get("snmp.trap.oid"); oid.Str() == linkDownOID {
				return true
			}
			v, _ := lr.Attributes().Get("snmp.trap.version")
			versions = append(versions, v.Str())
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "sentinel trap was not received")
	return versions
}

// allocateUDPPort finds an available UDP port on localhost.
func allocateUDPPort(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	addr := conn.LocalAddr().String()
	require.NoError(t, conn.Close())
	return addr
}

// waitForListenAddr polls until the trapper resolves its listen address.
func waitForListenAddr(t *testing.T, tr *Trapper) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := tr.ListenAddr(); addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for trapper to start")
	return ""
}

func TestTrapperStartStop(t *testing.T) {
	logger := zaptest.NewLogger(t)
	sink := &consumertest.LogsSink{}
	tr, err := New(logger, allocateUDPPort(t), nil, sink, newTestObsReport(t))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Run(ctx)
	}()

	addr := waitForListenAddr(t, tr)
	assert.NotEmpty(t, addr)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("trapper did not stop within timeout")
	}
}

func TestTrapperReceivesV2cTrap(t *testing.T) {
	addr, sink := startTrapper(t, []AuthEntry{{Version: "v2c", Community: "public"}})
	sendTrap(t, addr, v2cSender("public"), coldStartOID)

	attrs := firstRecordAttrs(t, sink)
	trapOID, ok := attrs.Get("snmp.trap.oid")
	require.True(t, ok, "expected snmp.trap.oid attribute")
	assert.Equal(t, coldStartOID, trapOID.Str())
	_, ok = attrs.Get("snmp.trap.community")
	assert.False(t, ok, "community string must not be emitted")
}

func TestTrapperAuthentication(t *testing.T) {
	auth := []AuthEntry{
		{Version: "v2c", Community: "public"},
		{Version: "v3", Username: "noauth"},
		{Version: "v3", Username: "authuser", AuthProtocol: "SHA256", AuthPassphrase: "authpass123"},
		{Version: "v3", Username: "privuser", AuthProtocol: "SHA", AuthPassphrase: "authpass123", PrivacyProtocol: "AES", PrivacyPassphrase: "privpass123"},
	}

	tests := []struct {
		name   string
		sender sender
		want   string // expected snmp.trap.version, empty when the trap must be dropped
	}{
		{"v2c correct community", v2cSender("public"), "v2c"},
		{"v2c wrong community", v2cSender("wrong"), ""},
		{"v2c community prefix", v2cSender("publi"), ""},
		{"v1 correct community", v1Sender("public"), "v1"},
		{"v1 wrong community", v1Sender("wrong"), ""},
		{"v3 noAuthNoPriv", v3Sender("noauth", gosnmp.NoAuth, "", gosnmp.NoPriv, ""), "v3"},
		{"v3 unknown user", v3Sender("intruder", gosnmp.NoAuth, "", gosnmp.NoPriv, ""), ""},
		{"v3 authNoPriv", v3Sender("authuser", gosnmp.SHA256, "authpass123", gosnmp.NoPriv, ""), "v3"},
		{"v3 authNoPriv wrong passphrase", v3Sender("authuser", gosnmp.SHA256, "wrongpass123", gosnmp.NoPriv, ""), ""},
		{"v3 authNoPriv wrong protocol", v3Sender("authuser", gosnmp.MD5, "authpass123", gosnmp.NoPriv, ""), ""},
		{"v3 authNoPriv user sends noAuthNoPriv", v3Sender("authuser", gosnmp.NoAuth, "", gosnmp.NoPriv, ""), ""},
		{"v3 authPriv", v3Sender("privuser", gosnmp.SHA, "authpass123", gosnmp.AES, "privpass123"), "v3"},
		{"v3 authPriv wrong auth passphrase", v3Sender("privuser", gosnmp.SHA, "wrongpass123", gosnmp.AES, "privpass123"), ""},
		{"v3 authPriv wrong privacy passphrase", v3Sender("privuser", gosnmp.SHA, "authpass123", gosnmp.AES, "wrongpass123"), ""},
		{"v3 authPriv user sends authNoPriv", v3Sender("privuser", gosnmp.SHA, "authpass123", gosnmp.NoPriv, ""), ""},
		{"v3 authPriv user sends noAuthNoPriv", v3Sender("privuser", gosnmp.NoAuth, "", gosnmp.NoPriv, ""), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, sink := startTrapper(t, auth)
			sendTrap(t, addr, tt.sender, coldStartOID)

			got := receivedVersions(t, addr, sink, v2cSender("public"))
			if tt.want == "" {
				assert.Empty(t, got, "trap should have been dropped")
				return
			}
			assert.Equal(t, []string{tt.want}, got)
		})
	}
}

func TestTrapperV3TrapContent(t *testing.T) {
	addr, sink := startTrapper(t, []AuthEntry{
		{Version: "v3", Username: "privuser", AuthProtocol: "SHA512", AuthPassphrase: "authpass123", PrivacyProtocol: "AES256", PrivacyPassphrase: "privpass123"},
	})
	sendTrap(t, addr, v3Sender("privuser", gosnmp.SHA512, "authpass123", gosnmp.AES256, "privpass123"), coldStartOID)

	attrs := firstRecordAttrs(t, sink)
	trapOID, _ := attrs.Get("snmp.trap.oid")
	assert.Equal(t, coldStartOID, trapOID.Str())
	trapType, _ := attrs.Get("snmp.trap.type")
	assert.Equal(t, "coldStart", trapType.Str())
}

func TestTrapperEmptyAcceptedAuth(t *testing.T) {
	addr, sink := startTrapper(t, nil)
	sendTrap(t, addr, v2cSender("anything"), coldStartOID)
	sendTrap(t, addr, v1Sender("other"), coldStartOID)
	sendTrap(t, addr, v3Sender("someone", gosnmp.NoAuth, "", gosnmp.NoPriv, ""), coldStartOID)

	// SNMPv1/v2c traps are accepted with any community; SNMPv3 traps cannot be
	// verified without a configured user and are dropped.
	got := receivedVersions(t, addr, sink, v2cSender("sentinel"))
	assert.ElementsMatch(t, []string{"v2c", "v1"}, got)
}

func TestIsAuthorizedV3HeaderChecks(t *testing.T) {
	tr := &Trapper{}
	newPacket := func(flags gosnmp.SnmpV3MsgFlags, model gosnmp.SnmpV3SecurityModel, privProto gosnmp.SnmpV3PrivProtocol) *gosnmp.SnmpPacket {
		return &gosnmp.SnmpPacket{
			Version:       gosnmp.Version3,
			MsgFlags:      flags,
			SecurityModel: model,
			SecurityParameters: &gosnmp.UsmSecurityParameters{
				UserName:               "user",
				AuthenticationProtocol: gosnmp.SHA,
				PrivacyProtocol:        privProto,
			},
		}
	}

	assert.True(t, tr.isAuthorized(newPacket(gosnmp.AuthNoPriv, gosnmp.UserSecurityModel, gosnmp.NoPriv), false))
	assert.False(t, tr.isAuthorized(newPacket(gosnmp.AuthNoPriv, 1, gosnmp.NoPriv), false), "non-USM security model")
	assert.False(t, tr.isAuthorized(newPacket(gosnmp.AuthPriv&^gosnmp.AuthNoPriv, gosnmp.UserSecurityModel, gosnmp.NoPriv), false), "priv without auth flags")
	assert.True(t, tr.isAuthorized(newPacket(gosnmp.AuthPriv|gosnmp.Reportable, gosnmp.UserSecurityModel, gosnmp.AES), true))
	assert.False(t, tr.isAuthorized(newPacket(gosnmp.AuthPriv, gosnmp.UserSecurityModel, gosnmp.AES), false), "plaintext scoped PDU under privacy flag")
}

func TestScopedPDUEncrypted(t *testing.T) {
	assert.True(t, scopedPDUEncrypted(captureTrap(t, v3Sender("u", gosnmp.SHA, "authpass123", gosnmp.AES, "privpass123"))))
	assert.False(t, scopedPDUEncrypted(captureTrap(t, v3Sender("u", gosnmp.SHA, "authpass123", gosnmp.NoPriv, ""))))
	assert.False(t, scopedPDUEncrypted(captureTrap(t, v2cSender("public"))))
	assert.False(t, scopedPDUEncrypted([]byte{0x30, 0x80}))
	// gosnmp accepts non-minimal BER lengths, so the check must too.
	assert.True(t, scopedPDUEncrypted([]byte{0x30, 0x81, 0x0c, 0x02, 0x01, 0x03, 0x30, 0x00, 0x04, 0x82, 0x00, 0x00, 0x04, 0x01, 0xff}))
}

func TestTrapperRejectsCraftedV3(t *testing.T) {
	auth := []AuthEntry{
		{Version: "v2c", Community: "public"},
		{Version: "v3", Username: "authuser", AuthProtocol: "SHA256", AuthPassphrase: "authpass123"},
	}
	tests := []struct {
		name string
		msg  []byte
	}{
		// gosnmp skips authentication for non-USM security models.
		{"non-USM security model", rawV3Trap("authuser", byte(gosnmp.AuthNoPriv), 1, string(make([]byte, 24)), true)},
		// A truncated message with empty authentication parameters makes
		// gosnmp slice past the end of the packet.
		{"truncated authenticated message", rawV3Trap("authuser", byte(gosnmp.AuthNoPriv), int(gosnmp.UserSecurityModel), "", false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, sink := startTrapper(t, auth)
			conn, err := net.Dial("udp", addr)
			require.NoError(t, err)
			_, err = conn.Write(tt.msg)
			require.NoError(t, err)
			require.NoError(t, conn.Close())

			assert.Empty(t, receivedVersions(t, addr, sink, v2cSender("public")), "crafted trap should have been dropped")
		})
	}
}

func TestTrapperAcknowledgesOnlyAuthorizedInforms(t *testing.T) {
	addr, sink := startTrapper(t, []AuthEntry{{Version: "v2c", Community: "public"}})

	assert.NoError(t, sendInform(t, addr, "public"))
	assert.Error(t, sendInform(t, addr, "wrong"), "unauthorized inform must not be acknowledged")
	assert.Equal(t, 1, sink.LogRecordCount())
}
