package connection

import (
	"strings"
	"sync"
)

// MockConnection is a test double for the Connection interface.
// It allows tests to set canned values and errors without a real SNMP agent.
type MockConnection struct {
	mu     sync.Mutex
	values map[string]interface{}
	err    error
}

// NewMockConnection creates a new MockConnection with empty values.
func NewMockConnection() *MockConnection {
	return &MockConnection{
		values: make(map[string]interface{}),
	}
}

// SetValues replaces the entire values map used to respond to Get and Walk calls.
func (m *MockConnection) SetValues(values map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values = values
}

// SetError configures the MockConnection to return the given error on all subsequent calls.
// Pass nil to clear a previously set error.
func (m *MockConnection) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Get returns the values from the mock for the requested OIDs.
// If an error has been set via SetError, it is returned instead.
// OIDs not present in the values map are silently omitted from the result.
func (m *MockConnection) Get(oids []string) (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}

	result := make(map[string]interface{}, len(oids))
	for _, oid := range oids {
		if v, ok := m.values[oid]; ok {
			result[oid] = v
		}
	}
	return result, nil
}

// Walk returns all values whose OID key has the given oid as a prefix (oid + ".").
// If an error has been set via SetError, it is returned instead.
func (m *MockConnection) Walk(oid string) (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}

	prefix := oid + "."
	result := make(map[string]interface{})
	for k, v := range m.values {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result, nil
}

// Close is a no-op on the mock and always returns nil.
func (m *MockConnection) Close() error {
	return nil
}
