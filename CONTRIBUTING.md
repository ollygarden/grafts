# Contributing

Thanks for contributing to Grafts — OllyGarden's collection of custom
OpenTelemetry Collector components. This guide covers project-wide conventions
for everyone (humans and coding agents). Coding agents should also read
[AGENTS.md](AGENTS.md) for the codebase map and the development workflow.

## Building and testing

From the repository root:

```bash
make test              # Run tests for all components
make test-integration  # Run Docker-backed integration tests (snmpreceiver)
make lint              # Run the linter for all components
make fmt               # Format all components
make tidy              # Run go mod tidy for all components
make build             # Build the test distribution
```

Run a single component's tests directly:

```bash
cd receiver/natsjetstreamreceiver
go test -v ./... -run TestName
```

**`make lint` is the source of truth before pushing.** golangci-lint runs
`errcheck` (e.g. unchecked error returns like a bare `defer f.Close()`) and
`staticcheck` (including `SA1019` deprecated-API usage) on top of `go vet` and
`go test` — issues have slipped past `go vet`/`go test` before. For a single
component: `golangci-lint run ./<receiver|exporter>/<component>/...`.

## Commit conventions

Use [Conventional Commits](https://www.conventionalcommits.org/). The scope is
the component directory:

- `feat(parquetexporter): add encryption at rest`
- `fix(snmpreceiver): handle empty walk response`
- `test(snmpreceiver): add integration test with snmpsim`
- `docs(parquetexporter): document DuckDB reads`

Dependency updates are managed by Renovate and land as `fix(deps):` or
`chore(deps):`. Reference the relevant Linear issue in the commit subject and in
the pull request.

## Pull requests

- Keep PRs small and focused on one change.
- Reference the Linear issue.
- CodeRabbit reviews each PR automatically; address its actionable comments
  (verify before applying) and reply in the thread.
- A human reviews and squash-merges. Keep the PR green (`make lint` + `make
  test`) before requesting review.

## Testing conventions

- Use [`stretchr/testify`](https://github.com/stretchr/testify): `require` for
  preconditions and fatal checks (it stops the test), `assert` for non-fatal
  assertions.
- Prefer table-driven tests with `t.Run` subtests.
- Use the Collector's Nop test helpers for component wiring:
  `componenttest.NewNopTelemetrySettings`, `exportertest.NewNopSettings`,
  `receivertest.NewNopSettings`.
- Prefer pure Go (no CGo) so the distribution stays portable.
- Docker-backed integration tests live behind `make test-integration`; keep unit
  tests runnable without external services.

## Instrumentation conventions

Components emit their own telemetry for self-observability: see
`exporter/parquetexporter/telemetry.go` for the pattern (named instruments such
as `parquetexporter.*`, with error classification by `error.type`). Follow
OpenTelemetry semantic conventions for attribute and metric names.

Instrumentation work is done using skills from two repositories:

**[`ollygarden/opentelemetry-agent-skills`](https://github.com/ollygarden/opentelemetry-agent-skills)** — upstream OpenTelemetry mechanics:

- `otel-collector` — authoring and configuring Collector components
- `otel-go` — OpenTelemetry Go API/SDK mechanics
- `otel-semantic-conventions` — attribute and metric naming
- `otel-sdk-versions` — selecting compatible OpenTelemetry module versions
- `otel-telemetrygen` — generating synthetic OTLP to test pipelines and components

**[`ollygarden/skills`](https://github.com/ollygarden/skills)** — OllyGarden's opinionated guides:

- `ollygarden-otel-instrumentation-planning` — deciding what and how to instrument
- `ollygarden-otel-go-setup` — Go SDK setup
- `ollygarden-otel-sdk-setup` — provider, exporter, and processor wiring
- `ollygarden-otel-manual-instrumentation` — adding spans, metrics, and logs by hand

## Go and tooling

- Go 1.25.
- Multi-module Go workspace; the distribution is assembled by the OpenTelemetry
  Collector Builder (OCB). Each component is its own module — see
  [AGENTS.md](AGENTS.md) for the module layout.
