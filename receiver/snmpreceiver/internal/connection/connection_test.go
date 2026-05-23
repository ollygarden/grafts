package connection

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockConnectionGet(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		".1.3.6.1.2.1.1.1.0": "Linux router",
		".1.3.6.1.2.1.1.5.0": "my-router",
		".1.3.6.1.2.1.1.6.0": "datacenter",
	})

	result, err := mock.Get([]string{".1.3.6.1.2.1.1.1.0", ".1.3.6.1.2.1.1.5.0"})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "Linux router", result[".1.3.6.1.2.1.1.1.0"])
	assert.Equal(t, "my-router", result[".1.3.6.1.2.1.1.5.0"])
}

func TestMockConnectionGetUnknownOID(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		".1.3.6.1.2.1.1.1.0": "Linux router",
	})

	result, err := mock.Get([]string{".9.9.9.9.9.9.9"})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestMockConnectionWalk(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		".1.3.6.1.2.1.2.2.1.1": 1,
		".1.3.6.1.2.1.2.2.1.2": 2,
		".1.3.6.1.2.1.2.2.1.3": 3,
		".1.3.6.1.9999.1.1":    "different subtree",
	})

	result, err := mock.Walk(".1.3.6.1.2.1.2.2.1")
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Contains(t, result, ".1.3.6.1.2.1.2.2.1.1")
	assert.Contains(t, result, ".1.3.6.1.2.1.2.2.1.2")
	assert.Contains(t, result, ".1.3.6.1.2.1.2.2.1.3")
	assert.NotContains(t, result, ".1.3.6.1.9999.1.1")
}

func TestMockConnectionError(t *testing.T) {
	mock := NewMockConnection()
	mock.SetValues(map[string]interface{}{
		".1.3.6.1.2.1.1.1.0": "Linux router",
	})

	expectedErr := errors.New("snmp timeout")
	mock.SetError(expectedErr)

	result, err := mock.Get([]string{".1.3.6.1.2.1.1.1.0"})
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, result)

	walkResult, walkErr := mock.Walk(".1.3.6.1.2.1.1")
	assert.ErrorIs(t, walkErr, expectedErr)
	assert.Nil(t, walkResult)
}

func TestNewConnectionParams(t *testing.T) {
	// Verify that the Params struct has the expected fields with the expected types.
	// This does not create a real connection.
	params := Params{
		Host:              "192.168.1.1",
		Port:              161,
		Version:           V2c,
		Community:         "public",
		Username:          "snmpuser",
		AuthProtocol:      "SHA",
		AuthPassphrase:    "authpass",
		PrivacyProtocol:   "AES",
		PrivacyPassphrase: "privpass",
		Timeout:           5 * time.Second,
		Retries:           3,
		MaxRepetitions:    25,
	}

	assert.Equal(t, "192.168.1.1", params.Host)
	assert.Equal(t, uint16(161), params.Port)
	assert.Equal(t, V2c, params.Version)
	assert.Equal(t, "public", params.Community)
	assert.Equal(t, "snmpuser", params.Username)
	assert.Equal(t, "SHA", params.AuthProtocol)
	assert.Equal(t, "authpass", params.AuthPassphrase)
	assert.Equal(t, "AES", params.PrivacyProtocol)
	assert.Equal(t, "privpass", params.PrivacyPassphrase)
	assert.Equal(t, 5*time.Second, params.Timeout)
	assert.Equal(t, 3, params.Retries)
	assert.Equal(t, uint32(25), params.MaxRepetitions)

	// Verify Version constants
	assert.Equal(t, Version("v2c"), V2c)
	assert.Equal(t, Version("v3"), V3)
}
