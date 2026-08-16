package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParsePrometheusAgainstACapturedScrape parses real exporter output rather
// than a fixture written to suit the parser.
func TestParsePrometheusAgainstACapturedScrape(t *testing.T) {
	f, err := os.Open(filepath.Join("testdata", "exporter-scrape.prom"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	series, err := ParsePrometheus(f)
	require.NoError(t, err)

	pools := series["pgbouncer_pools_server_active_connections"]
	assert.Equal(t, "gauge", pools.Type)
	assert.Equal(t, []string{"database", "user"}, pools.Labels)

	// A counter keeps its _total suffix: that is the name a user's dashboard
	// refers to, so deriving it would be guessing.
	queries := series["pgbouncer_stats_totals_queries_pooled_total"]
	assert.Equal(t, "counter", queries.Type)
	assert.Equal(t, []string{"database"}, queries.Labels)

	// The six-label databases family is the one the OTel shape trims.
	databases := series["pgbouncer_databases_pool_size"]
	assert.Equal(t, []string{"database", "force_user", "host", "name", "pool_mode", "port"}, databases.Labels)

	// An unlabelled series parses with no labels rather than being skipped.
	assert.Equal(t, "gauge", series["pgbouncer_pools"].Type)
	assert.Empty(t, series["pgbouncer_pools"].Labels)

	// The exporter's own runtime metrics parse too; excluding them is the
	// diff's job, by namespace, not the parser's.
	assert.Equal(t, "gauge", series["go_memstats_heap_objects"].Type)
	assert.Equal(t, []string{"code"}, series["promhttp_metric_handler_requests_total"].Labels)
}

func TestParsePrometheusHandlesCommasInLabelValues(t *testing.T) {
	// A label value containing a comma would split into bogus label keys under
	// a naive strings.Split.
	in := `# TYPE demo gauge
demo{app="a,b",env="prod"} 1
`
	series, err := ParsePrometheus(strings.NewReader(in))
	require.NoError(t, err)

	assert.Equal(t, []string{"app", "env"}, series["demo"].Labels)
}

func TestDiffClassifiesEveryOutcome(t *testing.T) {
	upstream := map[string]Series{
		"same":     {Name: "same", Type: "gauge", Labels: []string{"a"}},
		"gone":     {Name: "gone", Type: "gauge"},
		"retyped":  {Name: "retyped", Type: "counter", Labels: []string{"a"}},
		"relabels": {Name: "relabels", Type: "gauge", Labels: []string{"a", "b"}},
		"declined": {Name: "declined", Type: "gauge"},
	}
	ours := map[string]Series{
		"same":     {Name: "same", Type: "gauge", Labels: []string{"a"}},
		"retyped":  {Name: "retyped", Type: "gauge", Labels: []string{"a"}},
		"relabels": {Name: "relabels", Type: "gauge", Labels: []string{"a"}},
		"added":    {Name: "added", Type: "gauge"},
	}

	report := Diff(upstream, ours, Options{Dropped: map[string]string{"declined": "self-telemetry, not target telemetry"}})

	assert.Len(t, report.Matched, 1)
	assert.Len(t, report.Missing, 1)
	assert.Len(t, report.Extra, 1)
	assert.Len(t, report.ShapeMismatch, 2)
	assert.Len(t, report.Dropped, 1)

	// A declared drop is excluded from the denominator rather than counted
	// against us, and surface we add does not inflate the score.
	assert.InDelta(t, 1.0/4.0, report.Parity(), 1e-9)
}

func TestParityIsZeroWithNothingToCompare(t *testing.T) {
	assert.Zero(t, Report{}.Parity())
}

func TestMarkdownReportsEverySection(t *testing.T) {
	report := Diff(
		map[string]Series{"a": {Name: "a", Type: "gauge"}, "b": {Name: "b", Type: "gauge"}},
		map[string]Series{"a": {Name: "a", Type: "gauge"}},
		Options{},
	)

	md := report.Markdown("pgbouncerreceiver", "pgbouncer_exporter")
	for _, want := range []string{"Parity: 50.0%", "## Matched (1)", "## Missing (1)", "## Extra (0)", "DO NOT EDIT"} {
		assert.Contains(t, md, want)
	}
}

// TestDiffIgnoresMetricsOutsideTheTargetNamespace guards the reason Options has
// a Namespace at all: an exporter mounts the Go runtime and process collectors
// beside its own metrics, and those describe the exporter binary rather than
// the monitored system.
func TestDiffIgnoresMetricsOutsideTheTargetNamespace(t *testing.T) {
	upstream := map[string]Series{
		"pgbouncer_pools":          {Name: "pgbouncer_pools", Type: "gauge"},
		"go_memstats_heap_objects": {Name: "go_memstats_heap_objects", Type: "gauge"},
		"process_open_fds":         {Name: "process_open_fds", Type: "gauge"},
	}
	ours := map[string]Series{
		"pgbouncer_pools": {Name: "pgbouncer_pools", Type: "gauge"},
	}

	report := Diff(upstream, ours, Options{Namespace: "pgbouncer_"})

	assert.Len(t, report.Matched, 1)
	assert.Empty(t, report.Missing)
	assert.InDelta(t, 1.0, report.Parity(), 1e-9)
}
