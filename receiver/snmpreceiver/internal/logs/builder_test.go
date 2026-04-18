package logs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestBuildTrapLogLinkDown(t *testing.T) {
	trap := TrapData{
		SourceIP:   "192.168.1.1",
		SourcePort: 162,
		Version:    "v2c",
		Community:  "public",
		TrapOID:    "1.3.6.1.6.3.1.1.5.3",
		Uptime:     12345,
		Varbinds: map[string]interface{}{
			"1.3.6.1.2.1.2.2.1.1.1": 1,
		},
		Timestamp: time.Now(),
	}

	logs := BuildLog(trap)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)

	// Resource attributes
	resAttrs := rl.Resource().Attributes()
	hostVal, ok := resAttrs.Get("snmp.host")
	require.True(t, ok)
	assert.Equal(t, "192.168.1.1", hostVal.Str())

	portVal, ok := resAttrs.Get("snmp.port")
	require.True(t, ok)
	assert.Equal(t, int64(162), portVal.Int())

	require.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, scopeName, sl.Scope().Name())

	require.Equal(t, 1, sl.LogRecords().Len())
	lr := sl.LogRecords().At(0)

	assert.Equal(t, plog.SeverityNumberWarn, lr.SeverityNumber())
	assert.Contains(t, lr.Body().Str(), "1.3.6.1.6.3.1.1.5.3")

	attrs := lr.Attributes()

	oidVal, ok := attrs.Get("snmp.trap.oid")
	require.True(t, ok)
	assert.Equal(t, "1.3.6.1.6.3.1.1.5.3", oidVal.Str())

	trapType, ok := attrs.Get("snmp.trap.type")
	require.True(t, ok)
	assert.Equal(t, "linkDown", trapType.Str())

	version, ok := attrs.Get("snmp.trap.version")
	require.True(t, ok)
	assert.Equal(t, "v2c", version.Str())

	community, ok := attrs.Get("snmp.trap.community")
	require.True(t, ok)
	assert.Equal(t, "public", community.Str())
}

func TestBuildTrapLogColdStart(t *testing.T) {
	trap := TrapData{
		SourceIP:  "10.0.0.1",
		TrapOID:   "1.3.6.1.6.3.1.1.5.1",
		Timestamp: time.Now(),
	}

	logs := BuildLog(trap)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	sl := logs.ResourceLogs().At(0).ScopeLogs().At(0)
	require.Equal(t, 1, sl.LogRecords().Len())
	lr := sl.LogRecords().At(0)

	assert.Equal(t, plog.SeverityNumberInfo, lr.SeverityNumber())

	trapType, ok := lr.Attributes().Get("snmp.trap.type")
	require.True(t, ok)
	assert.Equal(t, "coldStart", trapType.Str())
}

func TestBuildTrapLogAuthFailure(t *testing.T) {
	trap := TrapData{
		SourceIP:  "10.0.0.2",
		Version:   "v3",
		TrapOID:   "1.3.6.1.6.3.1.1.5.5",
		Timestamp: time.Now(),
	}

	logs := BuildLog(trap)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	assert.Equal(t, plog.SeverityNumberError, lr.SeverityNumber())

	version, ok := lr.Attributes().Get("snmp.trap.version")
	require.True(t, ok)
	assert.Equal(t, "v3", version.Str())
}

func TestBuildTrapLogEnterpriseOID(t *testing.T) {
	enterpriseOID := "1.3.6.1.4.1.9.9.999.1"
	trap := TrapData{
		SourceIP:  "10.0.0.3",
		TrapOID:   enterpriseOID,
		Timestamp: time.Now(),
	}

	logs := BuildLog(trap)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	// Enterprise OIDs default to WARN
	assert.Equal(t, plog.SeverityNumberWarn, lr.SeverityNumber())

	// No snmp.trap.type attribute for unknown OIDs
	_, ok := lr.Attributes().Get("snmp.trap.type")
	assert.False(t, ok, "expected no snmp.trap.type attribute for enterprise OID")
}

func TestBuildTrapLogVarbinds(t *testing.T) {
	trap := TrapData{
		SourceIP: "10.0.0.4",
		TrapOID:  "1.3.6.1.6.3.1.1.5.3",
		Varbinds: map[string]interface{}{
			"1.3.6.1.2.1.2.2.1.1.2": 42,
			"1.3.6.1.2.1.2.2.1.2.2": "GigabitEthernet0/0",
		},
		Timestamp: time.Now(),
	}

	logs := BuildLog(trap)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	attrs := lr.Attributes()

	v1, ok := attrs.Get("snmp.varbind.1.3.6.1.2.1.2.2.1.1.2")
	require.True(t, ok)
	assert.Equal(t, "42", v1.Str())

	v2, ok := attrs.Get("snmp.varbind.1.3.6.1.2.1.2.2.1.2.2")
	require.True(t, ok)
	assert.Equal(t, "GigabitEthernet0/0", v2.Str())
}
