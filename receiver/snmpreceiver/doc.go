// Package snmpreceiver implements a receiver that polls SNMP targets for
// metrics and listens for SNMP traps/informs as logs.
//
// The receiver supports SNMPv2c and SNMPv3, with configurable metric groups
// that define which OIDs to collect and how to map them to OpenTelemetry
// metrics. SNMP traps are converted to OpenTelemetry log records.
package snmpreceiver // import "go.olly.garden/grafts/receiver/snmpreceiver"
