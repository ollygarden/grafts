package registrycheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const componentYAML = `
component: pgbouncerreceiver
system: pgbouncer
stability:
  semconv_version: v1.44.0
  stable:
    - db.namespace
  development:
    - db.client.connection.count
    - db.client.connection.state
`

// resolvedWith builds the shape Weaver's resolve output has, with one refined
// upstream metric carrying the given attributes.
func resolvedWith(metricStability string, attrs string) []byte {
	return []byte(`{
  "schema_url": "https://olly.garden/schemas/grafts/pgbouncerreceiver/0.1.0",
  "registry": {"metrics": []},
  "refinements": {"metrics": [{
    "name": "db.client.connection.count",
    "stability": "` + metricStability + `",
    "provenance": {"path": "/registry/metrics.yaml"},
    "attributes": [` + attrs + `]
  }]}
}`)
}

const disclosedAttr = `{"key": "db.client.connection.state", "stability": "development",
  "provenance": {"source": "https://opentelemetry.io/schemas/1.44.0", "path": "/semconv/db/registry.yaml"}}`

const undisclosedAttr = `{"key": "network.io.direction", "stability": "release_candidate",
  "provenance": {"source": "https://opentelemetry.io/schemas/1.44.0", "path": "/semconv/network/registry.yaml"}}`

const localAttr = `{"key": "pgbouncer.user", "stability": "development",
  "provenance": {"path": "/registry/attributes.yaml"}}`

const stableAttr = `{"key": "db.namespace", "stability": "stable",
  "provenance": {"source": "https://opentelemetry.io/schemas/1.44.0", "path": "/semconv/db/registry.yaml"}}`

func TestStabilityDisclosurePassesWhenEverythingIsListed(t *testing.T) {
	err := StabilityDisclosure(resolvedWith("development", disclosedAttr+","+stableAttr), []byte(componentYAML))
	assert.NoError(t, err)
}

func TestStabilityDisclosureFailsOnAnUndisclosedConvention(t *testing.T) {
	// A release-candidate key can still be renamed, so leaning on one without
	// saying so means nobody re-reviews it on a semconv bump.
	err := StabilityDisclosure(resolvedWith("development", disclosedAttr+","+undisclosedAttr), []byte(componentYAML))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "network.io.direction")
	assert.Contains(t, err.Error(), "release_candidate")
}

func TestStabilityDisclosureIgnoresLocalDeclarations(t *testing.T) {
	// A local key's stability is ours to choose and change; the disclosure is
	// about depending on someone else's churn.
	err := StabilityDisclosure(resolvedWith("development", localAttr), []byte(componentYAML))
	assert.NoError(t, err)
}

func TestStabilityDisclosureIgnoresStableConventions(t *testing.T) {
	err := StabilityDisclosure(resolvedWith("development", stableAttr), []byte(componentYAML))
	assert.NoError(t, err)
}

func TestStabilityDisclosureFailsOnAnUndisclosedMetricName(t *testing.T) {
	yaml := `
component: pgbouncerreceiver
system: pgbouncer
stability:
  stable: []
`
	err := StabilityDisclosure(resolvedWith("development", stableAttr), []byte(yaml))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db.client.connection.count")
}

func TestStabilityDisclosureRejectsMalformedInput(t *testing.T) {
	require.Error(t, StabilityDisclosure([]byte("not json"), []byte(componentYAML)))
	require.Error(t, StabilityDisclosure(resolvedWith("development", ""), []byte("\tnot: [yaml")))
}
