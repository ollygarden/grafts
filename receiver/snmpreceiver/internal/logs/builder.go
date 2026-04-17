// Package logs provides utilities for building plog.Logs from SNMP trap data.
package logs

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const scopeName = "go.olly.garden/grafts/receiver/snmpreceiver"

// TrapData holds all data parsed from a received SNMP trap.
type TrapData struct {
	SourceIP   string
	SourcePort int
	Version    string // "v1", "v2c", "v3"
	Community  string
	TrapOID    string
	Uptime     int64
	Varbinds   map[string]interface{}
	Timestamp  time.Time
}

// Well-known trap OIDs with name and severity mapping.
var wellKnownTraps = map[string]struct {
	Name     string
	Severity plog.SeverityNumber
}{
	"1.3.6.1.6.3.1.1.5.1": {Name: "coldStart", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.2": {Name: "warmStart", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.3": {Name: "linkDown", Severity: plog.SeverityNumberWarn},
	"1.3.6.1.6.3.1.1.5.4": {Name: "linkUp", Severity: plog.SeverityNumberInfo},
	"1.3.6.1.6.3.1.1.5.5": {Name: "authenticationFailure", Severity: plog.SeverityNumberError},
}

// BuildLog constructs a plog.Logs from the given TrapData.
func BuildLog(trap TrapData) plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	resource := rl.Resource()
	resource.Attributes().PutStr("snmp.host", trap.SourceIP)
	resource.Attributes().PutInt("snmp.port", int64(trap.SourcePort))

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(scopeName)

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(trap.Timestamp))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Determine severity from well-known trap lookup; default to WARN.
	severity := plog.SeverityNumberWarn
	if info, ok := wellKnownTraps[trap.TrapOID]; ok {
		severity = info.Severity
	}
	lr.SetSeverityNumber(severity)

	lr.Body().SetStr("SNMP Trap: " + trap.TrapOID)

	attrs := lr.Attributes()
	attrs.PutStr("snmp.trap.oid", trap.TrapOID)
	attrs.PutStr("snmp.trap.version", trap.Version)

	if trap.Community != "" {
		attrs.PutStr("snmp.trap.community", trap.Community)
	}

	if trap.Uptime > 0 {
		attrs.PutInt("snmp.uptime", trap.Uptime)
	}

	if info, ok := wellKnownTraps[trap.TrapOID]; ok {
		attrs.PutStr("snmp.trap.type", info.Name)
	}

	for oid, value := range trap.Varbinds {
		attrs.PutStr("snmp.varbind."+oid, fmt.Sprintf("%v", value))
	}

	return logs
}
