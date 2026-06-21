# CONTRIBUTING.md + AGENTS.md Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `CONTRIBUTING.md` into a real contributor guide, move the codebase map from `CLAUDE.md` into a new `AGENTS.md` with a documented development workflow, and reduce `CLAUDE.md` to a pointer.

**Architecture:** Documentation only. Three files: `CONTRIBUTING.md` (canonical conventions for everyone), `AGENTS.md` (codebase map + agent workflow, links to CONTRIBUTING for shared how-to), `CLAUDE.md` (pointer to AGENTS.md). No code, no tests, no build/lint impact.

**Tech Stack:** Markdown only.

## Global Constraints

- Documentation only — no `.go` files change; `make lint`/`make test` behavior is unaffected.
- No convention documented in two places: build/test/lint commands and conventions live only in `CONTRIBUTING.md`; `AGENTS.md` links to it.
- The 9 instrumentation skills must be listed under the correct repos: `otel-collector`, `otel-go`, `otel-semantic-conventions`, `otel-sdk-versions`, `otel-telemetrygen` under `ollygarden/opentelemetry-agent-skills`; `ollygarden-otel-instrumentation-planning`, `ollygarden-otel-go-setup`, `ollygarden-otel-sdk-setup`, `ollygarden-otel-manual-instrumentation` under `ollygarden/skills`.
- Do not document one-off incidents/quirks. Conventions only.
- Markdown must be well-formed: every fenced block has a language and is balanced; relative links resolve to real files.
- Make targets (verbatim, from the Makefile): `build`, `fmt`, `lint`, `test`, `test-integration`, `tidy`.
- Spec: `docs/superpowers/specs/2026-06-21-contributing-agents-docs-design.md`.

## File Structure

- `CONTRIBUTING.md` (overwrite the `TODO` stub) — canonical conventions for all contributors.
- `AGENTS.md` (create) — codebase map moved from `CLAUDE.md` (minus build commands) + Development workflow.
- `CLAUDE.md` (overwrite) — short pointer to `AGENTS.md`.

Task ordering: CONTRIBUTING.md first (AGENTS.md links to it), then the AGENTS.md/CLAUDE.md swap as one atomic task (CLAUDE.md must not be gutted while AGENTS.md is absent).

---

### Task 1: Write CONTRIBUTING.md

**Files:**
- Modify (overwrite): `CONTRIBUTING.md`

**Interfaces:**
- Produces: a `CONTRIBUTING.md` that `AGENTS.md` (Task 2) links to for build/test/lint and conventions.

- [ ] **Step 1: Overwrite `CONTRIBUTING.md` with the full guide**

Replace the entire file contents with:

```markdown
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
```

- [ ] **Step 2: Verify markdown is well-formed and links resolve**

Run:
```bash
cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts
# fenced code blocks balanced (count of ``` lines is even):
awk '/^```/{c++} END{print "fences:", c, (c%2==0?"OK":"UNBALANCED")}' CONTRIBUTING.md
# linked local files exist:
ls AGENTS.md >/dev/null 2>&1 && echo "AGENTS.md exists" || echo "AGENTS.md not yet created (created in Task 2)"
```
Expected: `fences: <even number> OK`. `AGENTS.md not yet created` is expected here — Task 2 creates it.

- [ ] **Step 3: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "docs: write CONTRIBUTING.md contributor guide (E-2354)"
```

---

### Task 2: Create AGENTS.md and reduce CLAUDE.md to a pointer

**Files:**
- Create: `AGENTS.md`
- Modify (overwrite): `CLAUDE.md`

**Interfaces:**
- Consumes: `CONTRIBUTING.md` from Task 1 (linked for build/test/lint and conventions).

- [ ] **Step 1: Create `AGENTS.md`**

Write the file with this content (the codebase map moved from `CLAUDE.md` minus
build commands, plus the Development workflow):

```markdown
# AGENTS.md

Guidance for coding agents working in this repository. **Read
[CONTRIBUTING.md](CONTRIBUTING.md) first** for project-wide conventions
(building, testing, commits, instrumentation). This file is the codebase map and
the development workflow.

## Overview

Grafts is a collection of custom OpenTelemetry Collector components for
OllyGarden. Components are "grafted" onto the standard collector via the
OpenTelemetry Collector Builder (OCB).

## Development workflow

Applies to **feature or behavior changes** (new components, features, behavior
changes). Trivial fixes (typos, comments, tiny localized bugfixes) skip
brainstorm/spec/plan and may go straight to a branch and PR. Dependency PRs use
the merge-bot skill.

1. Brainstorm the design with `superpowers:brainstorming` → spec in
   `docs/superpowers/specs/`.
2. Write the implementation plan with `superpowers:writing-plans` → plan in
   `docs/superpowers/plans/`.
3. Create a Linear issue on the **Engineering** team.
4. Branch off `main` using the branch name Linear suggests — branch *before* the
   spec and plan are committed, so design docs land on the feature branch.
5. Implement with `superpowers:subagent-driven-development` (per-task TDD plus
   spec/quality review, then a final whole-branch review).
6. `make lint` and `make test` must pass (see CONTRIBUTING.md).
7. Open a PR referencing the Linear issue; include a `Co-Authored-By` trailer on
   agent commits.
8. Address CodeRabbit comments with `superpowers:receiving-code-review` — verify
   each before applying; reply in the thread.
9. A human reviews and squash-merges. The agent never merges.
10. After merge, set the Linear issue to Done and delete the merged branch.

## Architecture

### Module Structure

The repository uses a multi-module Go workspace:

- Root module: `go.olly.garden/grafts` (placeholder)
- Component modules: each component (e.g. `receiver/natsjetstreamreceiver`) is a
  separate Go module with its own `go.mod`.

This structure is required by OCB, which references components as separate
modules with `path:` for local development and `replaces:` directives.

### Distribution

The `distributions/grafts/` directory contains:

- `manifest.yaml`: OCB manifest defining included components (receivers,
  processors, exporters, extensions, connectors, providers)
- `config.yaml`: sample collector configuration
- `Makefile`: build automation using the `builder` CLI

OCB generates the collector binary in `distributions/grafts/build/grafts`. To
run it: `cd distributions/grafts && make run` (or `make validate`).

### Components

**NATS JetStream Receiver** (`receiver/natsjetstreamreceiver/`):

- Consumes traces, metrics, and logs from NATS JetStream using pull-based consumers
- Uses a shared receiver pattern (single NATS connection for all signal types)
- Expects OTLP protobuf format on configured subjects
- Supports JetStream domains for clustered NATS deployments

Key files: `config.go` (config + validation), `factory.go` (shared instance via
`sync.Once`), `receiver.go` (two-phase init with graceful shutdown).

**NATS JetStream Exporter** (`exporter/natsjetstreamexporter/`):

- Publishes traces, metrics, and logs to NATS JetStream streams
- Sync and async publishing modes for throughput/reliability trade-offs
- OTLP protobuf format, compatible with the receiver above
- Supports JetStream domains for clustered NATS deployments

Key files: `config.go`, `factory.go`, `exporter.go` (publishing with sync/async
modes and error classification).

**Parquet Exporter** (`exporter/parquetexporter/`):

- Writes traces, metrics, and logs to local Parquet files for DuckDB consumption
- Pure Go (no CGo) via apache/arrow-go; DuckDB reads via `read_parquet()`
- Schema mirrors the ClickHouse exporter: traces (+events/links), logs, and five
  metric files (gauge/sum/histogram/exponential_histogram/summary)
- Attribute maps stored as JSON strings; files rotate on time/rows/bytes with
  atomic `.part` → `.parquet` rename
- Optional encryption at rest (Parquet Modular Encryption, AES-GCM)
- Emits its own metrics (`parquetexporter.*`) for rotation, rows/bytes, and I/O
  errors

Key files: `config.go`, `telemetry.go` (self-telemetry + error classification),
`schema.go` (Arrow schemas), `writer.go` (rotating writer with atomic rename),
`traces.go`/`logs.go`/`metrics.go` (OTLP → Arrow transforms), `exporter.go`
(lifecycle, flush ticker, push methods).

**SNMP Receiver** (`receiver/snmpreceiver/`):

- Polls SNMP targets for metrics and listens for traps/informs as logs
- Supports SNMPv2c and SNMPv3 with named, reusable auth configurations
- Metric groups define OID collections with table walks, index extraction, and
  lookup chains
- Trap listener converts SNMP traps to OTel log records with severity mapping
- Uses `gosnmp/gosnmp` (pure Go, no CGo)

Key files: `config.go`, `factory.go` (shared instance for metrics + logs),
`receiver.go` (orchestrator), `internal/connection/` (gosnmp wrapper + mock),
`internal/poller/` (scheduler + collector), `internal/trapper/` (UDP trap
listener), `internal/metrics/` (pmetric builder), `internal/logs/` (plog builder).

## Configuration

**NATS JetStream Receiver** requires: `url`, `stream`, `consumer_name`, `domain`,
and `subjects.traces/metrics/logs`.

**NATS JetStream Exporter** requires: `url`, `stream`, `domain`,
`subjects.traces/metrics/logs`, and `publish_async` (default: true).

**Parquet Exporter** requires `directory`; optional `flush_interval` (5m),
`max_rows` (100000), `max_bytes` (128000000), `compression` (zstd/snappy/none),
and `encryption` (base64 AES key + optional `key_id`).

**SNMP Receiver** requires: `auth` (named v2c/v3 configs), `targets`,
`metric_groups`, optional `trap_listener`, `collection_interval` (60s), `timeout`
(5s).
```

- [ ] **Step 2: Overwrite `CLAUDE.md` with a pointer**

Replace the entire file contents with:

```markdown
# CLAUDE.md

This project's instructions for coding agents live in [AGENTS.md](AGENTS.md).
Read it first — it is the codebase map and development workflow, and it points to
[CONTRIBUTING.md](CONTRIBUTING.md) for project-wide conventions.
```

- [ ] **Step 3: Verify structure, links, and no duplication**

Run:
```bash
cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts
# pointer chain:
grep -q 'AGENTS.md' CLAUDE.md && echo "CLAUDE.md -> AGENTS.md OK"
grep -q 'CONTRIBUTING.md' AGENTS.md && echo "AGENTS.md -> CONTRIBUTING.md OK"
# build commands are NOT duplicated into AGENTS.md (canonical in CONTRIBUTING.md):
grep -q 'make test-integration' AGENTS.md && echo "DUPLICATION: build commands leaked into AGENTS.md" || echo "no build-command duplication in AGENTS.md OK"
# fenced blocks balanced in both files:
for f in AGENTS.md CLAUDE.md; do awk '/^```/{c++} END{print FILENAME, "fences:", c, (c%2==0?"OK":"UNBALANCED")}' $f; done
# CLAUDE.md is now a short pointer (small):
wc -l CLAUDE.md
```
Expected: both pointer lines print OK; `no build-command duplication in AGENTS.md OK`; both files report an even fence count; `CLAUDE.md` is only a handful of lines.

- [ ] **Step 4: Sanity-check the Go build is unaffected (docs-only change)**

Run:
```bash
cd /Users/jpkroehling/Projects/src/github.com/ollygarden/grafts && git status --short
```
Expected: only `AGENTS.md` (new) and `CLAUDE.md` (modified) staged/changed — no `.go` files. (No need to run `make lint`/`make test`: nothing in Go changed.)

- [ ] **Step 5: Commit**

```bash
git add AGENTS.md CLAUDE.md
git commit -m "docs: move codebase map to AGENTS.md, point CLAUDE.md at it (E-2354)"
```

---

## Self-Review

**Spec coverage:**
- CONTRIBUTING.md as canonical conventions (build/test, commits, PRs, testing,
  instrumentation, Go/tooling) → Task 1. ✓
- The 9 instrumentation skills under the correct two repos → Task 1 Step 1. ✓
- AGENTS.md = moved codebase map (minus build commands) + Development workflow +
  "read CONTRIBUTING.md first" → Task 2 Step 1. ✓
- CLAUDE.md reduced to a pointer → Task 2 Step 2. ✓
- No duplication of build commands → enforced by Task 2 Step 3 check. ✓
- Markdown well-formed / links resolve → verification in Task 1 Step 2 and Task 2
  Step 3. ✓

**Placeholder scan:** No TBD/TODO; both files' full content is inline. The
`AGENTS.md not yet created` message in Task 1 Step 2 is an expected interim state,
not a placeholder.

**Consistency:** File names (`CONTRIBUTING.md`, `AGENTS.md`, `CLAUDE.md`), the
9-skill list, and the 10-step workflow match the spec and across tasks. The
"branch before committing spec/plan" rule (workflow step 4) matches the spec.

## Workflow reminder (from the spec)

Surrounding workflow (already underway): Linear E-2354 (Engineering, In Progress);
branch `jpkroehling/e-2354-contributing-agents-docs`; implement via
subagent-driven development; PR referencing E-2354; CodeRabbit; human squash-merge;
then set E-2354 Done and delete the branch.
