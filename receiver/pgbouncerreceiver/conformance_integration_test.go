//go:build integration

package pgbouncerreceiver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"go.olly.garden/grafts/internal/conformance"
	"go.olly.garden/grafts/internal/promcompat"
)

// Pinned by digest: a parity number measured against a moving image is not a
// measurement. These are recorded in telemetry/component.yaml too.
const (
	postgresImage  = "postgres:17-alpine@sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73"
	pgbouncerImage = "edoburu/pgbouncer:v1.25.2-p0@sha256:7d7a27d9e90985cab5cf42256f5c13a3120baa4b055b69df37beb272b89b2340"
	exporterImage  = "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1@sha256:30f31b6c2efdad3647f8182cc7c1a3a19e42bae5d17387694989f969371c230d"
)

// dropped are the upstream series component.yaml accounts for. They are
// excluded from the parity denominator rather than counted against it, which is
// what keeps the number honest.
var dropped = map[string]string{
	"pgbouncer_exporter_build_info": "describes the upstream exporter binary this component replaces",
}

// TestConformance is the measurement the pilot exists to produce: run the real
// exporter and this receiver against the same PgBouncer, diff the two
// Prometheus-shaped outputs, and write the result to parity-report.md.
func TestConformance(t *testing.T) {
	ctx := t.Context()
	requireDocker(ctx, t)

	net, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	startPostgres(ctx, t, net.Name)
	pgbouncerHost, pgbouncerPort := startPgBouncer(ctx, t, net.Name)
	exporterURL := startExporter(ctx, t, net.Name)

	// Drive traffic first: pools, clients and stats are all empty on an idle
	// instance, and a diff of two empty scrapes proves nothing.
	generateLoad(ctx, t, net.Name)

	upstream := scrapeExporter(t, exporterURL)
	ours := scrapeReceiver(ctx, t, fmt.Sprintf("%s:%s", pgbouncerHost, pgbouncerPort))

	report := conformance.Diff(upstream, ours, conformance.Options{
		Namespace: "pgbouncer_",
		Dropped:   dropped,
	})

	path := filepath.Join(".", "parity-report.md")
	require.NoError(t, os.WriteFile(path, []byte(report.Markdown("pgbouncerreceiver", "pgbouncer_exporter")), 0o644))
	t.Logf("parity %.1f%% -- %d matched, %d missing, %d shape-mismatch, %d extra; wrote %s",
		report.Parity()*100, len(report.Matched), len(report.Missing), len(report.ShapeMismatch), len(report.Extra), path)

	for _, s := range report.Missing {
		t.Logf("missing: %s", s.Shape())
	}
	for _, m := range report.ShapeMismatch {
		t.Logf("shape mismatch:\n  upstream %s\n  ours     %s", m.Upstream.Shape(), m.Ours.Shape())
	}

	// The floor is enforced once component.yaml sets one. Until the first
	// measurement lands there is nothing honest to assert beyond having
	// produced a number, so this only guards against a total failure to emit.
	assert.Positive(t, len(report.Matched), "the compat scope produced nothing to compare")

	if floor := parityFloor(t); floor > 0 {
		assert.GreaterOrEqual(t, report.Parity(), floor,
			"parity fell below the floor in telemetry/component.yaml")
	}
}

func requireDocker(ctx context.Context, t *testing.T) {
	t.Helper()

	// Skip only when Docker itself is unreachable; a failing container or a
	// missing image must fail the test rather than quietly pass.
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(ctx); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
}

func startPostgres(ctx context.Context, t *testing.T, netName string) {
	t.Helper()

	run(ctx, t, testcontainers.ContainerRequest{
		Image:          postgresImage,
		Networks:       []string{netName},
		NetworkAliases: map[string][]string{netName: {"postgres"}},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(90 * time.Second),
	})
}

func startPgBouncer(ctx context.Context, t *testing.T, netName string) (string, string) {
	t.Helper()

	// Copied into the container rather than bind-mounted: a bind mount would
	// need an SELinux relabel of the checkout, which a test has no business
	// doing to a developer's working tree.
	container := run(ctx, t, testcontainers.ContainerRequest{
		Image:          pgbouncerImage,
		Networks:       []string{netName},
		NetworkAliases: map[string][]string{netName: {"pgbouncer"}},
		ExposedPorts:   []string{"6432/tcp"},
		Files: []testcontainers.ContainerFile{
			{HostFilePath: "testdata/fixture/pgbouncer.ini", ContainerFilePath: "/etc/pgbouncer/pgbouncer.ini", FileMode: 0o644},
			{HostFilePath: "testdata/fixture/userlist.txt", ContainerFilePath: "/etc/pgbouncer/userlist.txt", FileMode: 0o644},
		},
		WaitingFor: wait.ForLog("listening on 0.0.0.0:6432").WithStartupTimeout(60 * time.Second),
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6432/tcp")
	require.NoError(t, err)
	return host, port.Port()
}

func startExporter(ctx context.Context, t *testing.T, netName string) string {
	t.Helper()

	container := run(ctx, t, testcontainers.ContainerRequest{
		Image:        exporterImage,
		Networks:     []string{netName},
		ExposedPorts: []string{"9127/tcp"},
		Cmd: []string{
			"--pgBouncer.connectionString=postgres://pgbouncer:pgbouncer@pgbouncer:6432/pgbouncer?sslmode=disable",
		},
		WaitingFor: wait.ForHTTP("/metrics").WithPort("9127/tcp").WithStartupTimeout(60 * time.Second),
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9127/tcp")
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:%s/metrics", host, port.Port())
}

// generateLoad opens sessions through PgBouncer so pools, clients and stats are
// populated on both scrapes.
func generateLoad(ctx context.Context, t *testing.T, netName string) {
	t.Helper()

	for i := 0; i < 4; i++ {
		req := testcontainers.ContainerRequest{
			Image:    postgresImage,
			Networks: []string{netName},
			Env:      map[string]string{"PGPASSWORD": "testpass"},
			Cmd: []string{
				"psql", "-h", "pgbouncer", "-p", "6432", "-U", "testuser", "-d", "testdb",
				"-c", "select count(*) from generate_series(1,10000); select pg_sleep(4);",
			},
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}

	// Long enough for the sessions to be established and counted, short enough
	// that they are still open when both sides are scraped.
	time.Sleep(2 * time.Second)
}

func scrapeExporter(t *testing.T, url string) map[string]conformance.Series {
	t.Helper()

	resp, err := http.Get(url) //nolint:gosec // the URL is built from a container this test started
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Kept for debugging a diff: the committed capture is a different run.
	require.NoError(t, os.WriteFile(filepath.Join("testdata", "parity", "upstream.latest.prom"), body, 0o644))

	series, err := conformance.ParsePrometheus(bytes.NewReader(body))
	require.NoError(t, err)
	return series
}

func scrapeReceiver(ctx context.Context, t *testing.T, endpoint string) map[string]conformance.Series {
	t.Helper()

	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)
	cfg.Endpoint = endpoint
	cfg.Username = "pgbouncer"
	cfg.Password = "pgbouncer"
	cfg.CollectionInterval = time.Second
	cfg.Timeout = 500 * time.Millisecond
	cfg.Emit = []Shape{ShapeOTel, ShapePrometheus}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(NewFactory().Type())
	r, err := newReceiver(cfg, &settings, sink)
	require.NoError(t, err)

	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })

	require.Eventually(t, func() bool { return len(sink.AllMetrics()) > 0 }, 15*time.Second, 100*time.Millisecond)

	return conformance.FromMetrics(sink.AllMetrics()[0], scopeName+promcompat.ScopeSuffix)
}

// parityFloor reads the enforced floor from component.yaml. It is null until
// this test has produced a real number: the program's 90% default was a guess,
// and the pilot exists to replace it with a measurement.
func parityFloor(t *testing.T) float64 {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("telemetry", "component.yaml"))
	require.NoError(t, err)

	for _, line := range strings.Split(string(raw), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "floor:")
		if !ok {
			continue
		}
		var floor float64
		if _, err := fmt.Sscanf(strings.TrimSpace(rest), "%f", &floor); err != nil {
			return 0
		}
		return floor
	}
	return 0
}

func run(ctx context.Context, t *testing.T, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "starting %s", req.Image)
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return c
}
