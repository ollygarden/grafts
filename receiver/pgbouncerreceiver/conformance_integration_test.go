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

// TestConformance is the measurement the pilot exists to produce: run the real
// exporter and this receiver against the same PgBouncer, diff the two
// Prometheus-shaped outputs, and write the result to parity-report.md.
func TestConformance(t *testing.T) {
	ctx := t.Context()
	requireDocker(ctx, t)

	// Every parity fact -- the pinned images, the namespace, the declared
	// drops, the floor -- comes from component.yaml. A harness that restated
	// them could disagree with the file a reviewer reads.
	meta, err := conformance.LoadComponent(filepath.Join("telemetry", "component.yaml"))
	require.NoError(t, err)

	net, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	startPostgres(ctx, t, net.Name, meta.Fixture.Backend.Ref())
	pgbouncerHost, pgbouncerPort := startPgBouncer(ctx, t, net.Name, meta.Fixture.Target.Ref())
	exporterURL := startExporter(ctx, t, net.Name, meta.UpstreamRef())

	// Drive traffic first: pools, clients and stats are all empty on an idle
	// instance, and a diff of two empty scrapes proves nothing.
	generateLoad(ctx, t, net.Name, meta.Fixture.Backend.Ref())

	upstream := scrapeExporter(t, exporterURL)
	ours := scrapeReceiver(ctx, t, fmt.Sprintf("%s:%s", pgbouncerHost, pgbouncerPort))

	report := conformance.Diff(upstream, ours, meta.Options())

	path := filepath.Join(".", "parity-report.md")
	require.NoError(t, os.WriteFile(path, []byte(report.Markdown(meta.Component, filepath.Base(meta.Upstream.Repository))), 0o644))
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

	if meta.Parity.Floor > 0 {
		assert.GreaterOrEqual(t, report.Parity(), meta.Parity.Floor,
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

func startPostgres(ctx context.Context, t *testing.T, netName, image string) {
	t.Helper()

	run(ctx, t, testcontainers.ContainerRequest{
		Image:          image,
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

func startPgBouncer(ctx context.Context, t *testing.T, netName, image string) (string, string) {
	t.Helper()

	// Copied into the container rather than bind-mounted: a bind mount would
	// need an SELinux relabel of the checkout, which a test has no business
	// doing to a developer's working tree.
	container := run(ctx, t, testcontainers.ContainerRequest{
		Image:          image,
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

func startExporter(ctx context.Context, t *testing.T, netName, image string) string {
	t.Helper()

	container := run(ctx, t, testcontainers.ContainerRequest{
		Image:        image,
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
func generateLoad(ctx context.Context, t *testing.T, netName, image string) {
	t.Helper()

	for i := 0; i < 4; i++ {
		req := testcontainers.ContainerRequest{
			Image:    image,
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

	// Kept for debugging a failed diff, out of tree: rewriting a tracked file
	// would leave the working tree dirty against CI's diff gate.
	latest := filepath.Join(t.TempDir(), "upstream.latest.prom")
	require.NoError(t, os.WriteFile(latest, body, 0o644))
	t.Logf("upstream scrape written to %s", latest)

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
	cfg.Emit = promcompat.Emit{promcompat.ShapeOTel, promcompat.ShapePrometheus}
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
