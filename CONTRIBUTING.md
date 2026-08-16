# Contributing to Grafts

Thanks for contributing to Grafts, OllyGarden's collection of custom
OpenTelemetry Collector components. Check current issues and pull requests
before starting. For substantial features, component contracts, or distribution
changes, open an issue and align the approach with maintainers before investing
in implementation.

Coding agents must also read [AGENTS.md](AGENTS.md), which contains the codebase
map, repository workflow, guardrails, and change-specific validation matrix.

## Set up and validate

Use Go 1.26.6 to match CI; the module retains Go 1.26.6 compatibility. The
distribution build uses the OpenTelemetry Collector Builder. SNMP integration
tests require Docker and skip when it is unavailable.

```bash
make tidy
git diff --exit-code -- go.mod go.sum
make build
make lint
make test
make test-integration
git diff --check
test -z "${BASE_SHA:-}" || git diff --check "${BASE_SHA}...HEAD"
```

`make lint` is the source of truth before pushing. It includes checks such as
`errcheck` and `staticcheck` that are not covered by `go vet` or unit
tests. `make fmt` and `make tidy` modify files, so run them intentionally
and review their complete diff.

## Commits

Use Conventional Commits with a component scope when appropriate:

```text
feat(parquetexporter): add encryption at rest
fix(snmpreceiver): handle empty walk response
test(natsjetstreamreceiver): cover redelivery
docs(parquetexporter): document DuckDB reads
```

Keep the description concise and imperative. Use `!` and a
`BREAKING CHANGE:` footer for incompatible changes. Link the relevant GitHub
issue or, for OllyGarden employees, Linear issue when applicable. Renovate owns
routine dependency refreshes.

## Pull requests

Work from a focused branch in your fork or a maintainer-approved branch. Open a
pull request against current `main`. Pull-request titles follow Conventional
Commits because changes are squash-merged.

Each pull request should:

- solve one logical problem without unrelated refactors, dependency updates,
  generated artifacts, or broad formatting;
- summarize the motivation and approach;
- link the relevant issue when applicable;
- list exact validation commands and results, including skipped integration
  tests;
- call out compatibility, performance, security, and rollout risks, or state
  `None`;
- update component documentation, configuration examples, the OCB manifest,
  and design records when those contracts change.

CodeRabbit reviews pull requests. Verify actionable findings before applying
them and reply on each thread. A human reviewer squash-merges; contributors and
coding agents do not merge their own changes.

By contributing, you agree that your contribution is provided under this
repository's [Apache License 2.0](LICENSE). Never include credentials,
encryption keys, real telemetry, production exports, or data belonging to
users or customers.

## Testing conventions

- Use `stretchr/testify`: `require` for preconditions and fatal checks,
  `assert` for non-fatal assertions.
- Prefer table-driven tests with `t.Run` subtests.
- Use Collector nop helpers such as
  `componenttest.NewNopTelemetrySettings`, `exportertest.NewNopSettings`,
  and `receivertest.NewNopSettings` for component wiring.
- Prefer pure Go so the distribution stays portable.
- Keep unit tests independent of external services. Docker-backed SNMP tests
  live behind `make test-integration`.

## Instrumentation conventions

Components emit their own telemetry for self-observability. Follow the patterns
in component `telemetry.go` files, including bounded attributes and
`error.type` classification, and use OpenTelemetry semantic conventions for
attribute and metric names.

Useful OpenTelemetry references include the public
[`ollygarden/opentelemetry-agent-skills`](https://github.com/ollygarden/opentelemetry-agent-skills)
repository:

- `otel-collector` for Collector component authoring and configuration;
- `otel-go` for OpenTelemetry Go API and SDK mechanics;
- `otel-semantic-conventions` for attribute and metric naming;
- `otel-sdk-versions` for compatible module selection;
- `otel-telemetrygen` for synthetic OTLP generation and pipeline testing.

OllyGarden-maintained guidance in
[`ollygarden/skills`](https://github.com/ollygarden/skills) includes:

- `ollygarden-otel-instrumentation-planning`;
- `ollygarden-otel-go-setup`;
- `ollygarden-otel-sdk-setup`;
- `ollygarden-otel-manual-instrumentation`.

All contributors are responsible for understanding and validating their
submissions, including agent-generated work. Review the complete diff and
confirm that tests exercise the intended behavior before requesting review.
