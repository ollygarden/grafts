package trapper

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap/zaptest"
)

// sendTestTrap sends a test SNMPv2c trap to the given address.
func sendTestTrap(t *testing.T, addr, community, trapOID string) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	var port uint16
	_, err = fmt.Sscanf(portStr, "%d", &port)
	require.NoError(t, err)

	g := &gosnmp.GoSNMP{
		Target:    host,
		Port:      port,
		Version:   gosnmp.Version2c,
		Community: community,
		Timeout:   time.Second,
	}
	err = g.Connect()
	require.NoError(t, err)
	defer func() { _ = g.Conn.Close() }()

	trap := gosnmp.SnmpTrap{
		Variables: []gosnmp.SnmpPDU{
			{Name: "1.3.6.1.2.1.1.3.0", Type: gosnmp.TimeTicks, Value: uint32(123456)},
			{Name: snmpTrapOIDMIB, Type: gosnmp.ObjectIdentifier, Value: trapOID},
		},
	}
	_, err = g.SendTrap(trap)
	require.NoError(t, err)
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
	tr := New(logger, allocateUDPPort(t), nil, sink)

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
	logger := zaptest.NewLogger(t)
	sink := &consumertest.LogsSink{}
	auth := []AuthEntry{{Version: "v2c", Community: "public"}}
	tr := New(logger, allocateUDPPort(t), auth, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)

	addr := waitForListenAddr(t, tr)
	sendTestTrap(t, addr, "public", "1.3.6.1.6.3.1.1.5.1")

	// Wait for the log to arrive.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.LogRecordCount() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	require.Greater(t, sink.LogRecordCount(), 0, "expected at least one log record")

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	rl := allLogs[0].ResourceLogs().At(0)
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	trapOIDVal, ok := lr.Attributes().Get("snmp.trap.oid")
	require.True(t, ok, "expected snmp.trap.oid attribute")
	assert.Equal(t, "1.3.6.1.6.3.1.1.5.1", trapOIDVal.Str())
}

func TestTrapperRejectsWrongCommunity(t *testing.T) {
	logger := zaptest.NewLogger(t)
	sink := &consumertest.LogsSink{}
	auth := []AuthEntry{{Version: "v2c", Community: "secret"}}
	tr := New(logger, allocateUDPPort(t), auth, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)

	addr := waitForListenAddr(t, tr)
	sendTestTrap(t, addr, "wrong", "1.3.6.1.6.3.1.1.5.1")

	// Wait briefly and verify no logs were consumed.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, sink.LogRecordCount(), "expected no log records for wrong community")
}
