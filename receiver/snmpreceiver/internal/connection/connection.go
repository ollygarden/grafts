package connection

import (
	"fmt"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
)

// normalizeOID strips the leading dot that gosnmp adds to OID names.
func normalizeOID(oid string) string {
	return strings.TrimPrefix(oid, ".")
}

// Version represents the SNMP version to use.
type Version string

const (
	// V2c is SNMP version 2c.
	V2c Version = "v2c"
	// V3 is SNMP version 3.
	V3 Version = "v3"
)

// Params holds the parameters needed to create an SNMP connection.
type Params struct {
	Host              string
	Port              uint16
	Version           Version
	Community         string
	Username          string
	AuthProtocol      string
	AuthPassphrase    string
	PrivacyProtocol   string
	PrivacyPassphrase string
	Timeout           time.Duration
	Retries           int
	MaxRepetitions    uint32
}

// Connection defines the interface for interacting with an SNMP agent.
type Connection interface {
	// Get fetches OID values via SNMP GET and returns a map of OID to value.
	Get(oids []string) (map[string]interface{}, error)
	// Walk walks an OID subtree via SNMP WALK/BULKWALK and returns a map of OID to value.
	Walk(oid string) (map[string]interface{}, error)
	// Close closes the underlying SNMP connection.
	Close() error
}

// GosnmpWrapper wraps a *gosnmp.GoSNMP and implements the Connection interface.
type GosnmpWrapper struct {
	client *gosnmp.GoSNMP
}

// NewConnection creates a new GosnmpWrapper with the given parameters and connects to the SNMP agent.
func NewConnection(params Params) (Connection, error) {
	port := params.Port
	if port == 0 {
		port = 161
	}
	timeout := params.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	client := &gosnmp.GoSNMP{
		Target:         params.Host,
		Port:           port,
		Timeout:        timeout,
		Retries:        params.Retries,
		MaxRepetitions: params.MaxRepetitions,
	}

	switch params.Version {
	case V3:
		client.Version = gosnmp.Version3
		client.MsgFlags = gosnmp.AuthPriv
		client.SecurityModel = gosnmp.UserSecurityModel
		client.SecurityParameters = &gosnmp.UsmSecurityParameters{
			UserName:                 params.Username,
			AuthenticationProtocol:   mapAuthProtocol(params.AuthProtocol),
			AuthenticationPassphrase: params.AuthPassphrase,
			PrivacyProtocol:          mapPrivacyProtocol(params.PrivacyProtocol),
			PrivacyPassphrase:        params.PrivacyPassphrase,
		}
	default: // V2c and anything else defaults to V2c
		client.Version = gosnmp.Version2c
		client.Community = params.Community
	}

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("snmp connect: %w", err)
	}

	return &GosnmpWrapper{client: client}, nil
}

// Get fetches the given OIDs via SNMP GET and returns a map of OID to value.
// OIDs returning NoSuchObject or NoSuchInstance are silently filtered out.
func (w *GosnmpWrapper) Get(oids []string) (map[string]interface{}, error) {
	if len(oids) == 0 {
		return map[string]interface{}{}, nil
	}

	result, err := w.client.Get(oids)
	if err != nil {
		return nil, fmt.Errorf("snmp get: %w", err)
	}

	values := make(map[string]interface{}, len(result.Variables))
	for _, pdu := range result.Variables {
		if pdu.Type == gosnmp.NoSuchObject || pdu.Type == gosnmp.NoSuchInstance {
			continue
		}
		values[normalizeOID(pdu.Name)] = pdu.Value
	}
	return values, nil
}

// Walk walks the OID subtree via SNMP BULKWALK (v2c/v3) or WALK (v1) and returns a map of OID to value.
// OIDs returning NoSuchObject or NoSuchInstance are silently filtered out.
func (w *GosnmpWrapper) Walk(oid string) (map[string]interface{}, error) {
	values := make(map[string]interface{})

	walkFn := func(pdu gosnmp.SnmpPDU) error {
		if pdu.Type == gosnmp.NoSuchObject || pdu.Type == gosnmp.NoSuchInstance {
			return nil
		}
		values[normalizeOID(pdu.Name)] = pdu.Value
		return nil
	}

	var err error
	if w.client.Version == gosnmp.Version1 {
		err = w.client.Walk(oid, walkFn)
	} else {
		err = w.client.BulkWalk(oid, walkFn)
	}
	if err != nil {
		return nil, fmt.Errorf("snmp walk %s: %w", oid, err)
	}

	return values, nil
}

// Close closes the underlying SNMP connection.
func (w *GosnmpWrapper) Close() error {
	if w.client.Conn != nil {
		return w.client.Conn.Close()
	}
	return nil
}

// mapAuthProtocol maps a string auth protocol name to a gosnmp SnmpV3AuthProtocol.
func mapAuthProtocol(protocol string) gosnmp.SnmpV3AuthProtocol {
	switch strings.ToUpper(protocol) {
	case "SHA":
		return gosnmp.SHA
	case "SHA256":
		return gosnmp.SHA256
	case "SHA384":
		return gosnmp.SHA384
	case "SHA512":
		return gosnmp.SHA512
	case "MD5":
		return gosnmp.MD5
	default:
		return gosnmp.NoAuth
	}
}

// mapPrivacyProtocol maps a string privacy protocol name to a gosnmp SnmpV3PrivProtocol.
func mapPrivacyProtocol(protocol string) gosnmp.SnmpV3PrivProtocol {
	switch strings.ToUpper(protocol) {
	case "AES":
		return gosnmp.AES
	case "AES192":
		return gosnmp.AES192
	case "AES256":
		return gosnmp.AES256
	case "DES":
		return gosnmp.DES
	default:
		return gosnmp.NoPriv
	}
}
