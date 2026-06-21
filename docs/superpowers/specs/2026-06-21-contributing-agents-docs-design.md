# CONTRIBUTING.md + AGENTS.md restructure and documented dev workflow

Date: 2026-06-21
Linear: E-2354

## Goal

Make the repo's contributor and agent documentation explicit and reusable:

1. Turn `CONTRIBUTING.md` (currently a `TODO` stub) into a real guide for **all**
   contributors — humans and agents — covering project-wide conventions.
2. Move the current `CLAUDE.md` codebase guidance into `AGENTS.md` and add a
   **Development workflow** section capturing the brainstorm → spec → plan →
   Linear → branch → subagent-TDD → PR → CodeRabbit → human-merge flow.
3. Reduce `CLAUDE.md` to a pointer at `AGENTS.md`, so there is a single source of
   truth and Claude Code still loads project instructions.

## Non-goals

- No code changes; documentation only.
- No new skills or tooling; the workflow references existing `superpowers:*`
  skills rather than redefining them.
- Not documenting one-off incidents or quirks (e.g. a one-time branch
  divergence). Conventions only.

## File roles after this change

- **`CONTRIBUTING.md`** — canonical, audience = everyone. General conventions.
  No agent-process detail.
- **`AGENTS.md`** — audience = coding agents. The codebase map (moved verbatim
  from `CLAUDE.md`) plus the Development workflow. References `CONTRIBUTING.md`
  for shared conventions instead of duplicating them.
- **`CLAUDE.md`** — a short pointer telling Claude Code to read `AGENTS.md`.

## CONTRIBUTING.md outline

Grounded in what the repo already does (verified against git history and code):

1. **Building & testing** — `make test`, `make test-integration`, `make lint`,
   `make fmt`, `make tidy`, `make build`; per-component `go test ./... -run X`.
   **golangci-lint is the source of truth** (runs `errcheck` + `staticcheck`
   beyond `go vet`/`go test`); run it before pushing.
2. **Commit conventions** — Conventional Commits; scope = component directory
   (`feat(parquetexporter): …`, `fix(snmpreceiver): …`); dependency updates via
   Renovate land as `fix(deps):` / `chore(deps):`; reference the Linear issue in
   the subject and the PR.
3. **Pull requests** — small and focused; reference the Linear issue; CodeRabbit
   reviews automatically; a human merges (squash).
4. **Testing conventions** — `stretchr/testify` (`require` for preconditions /
   fatal checks, `assert` for non-fatal assertions); table-driven `t.Run`
   subtests; collector Nop helpers (`componenttest.NewNopTelemetrySettings`,
   `exportertest.NewNopSettings`, `receivertest.NewNopSettings`); prefer pure-Go
   (no CGo); Docker-backed integration tests gated behind `make test-integration`.
5. **Instrumentation conventions** — components emit their own telemetry via a
   `telemetry.go` (e.g. `parquetexporter.*` instruments with error
   classification) following OpenTelemetry semantic conventions. Instrumentation
   work uses skills from two repositories (see list below).
6. **Go & tooling** — Go 1.25; multi-module workspace assembled by the
   OpenTelemetry Collector Builder (OCB).

### Instrumentation skills (the 9)

State explicitly that instrumentation is done using skills from
`ollygarden/opentelemetry-agent-skills` and `ollygarden/skills`, grouped:

**From `ollygarden/opentelemetry-agent-skills`** (upstream OTel mechanics):
- `otel-collector` — authoring/configuring Collector components
- `otel-go` — OpenTelemetry Go API/SDK mechanics
- `otel-semantic-conventions` — attribute/metric naming
- `otel-sdk-versions` — selecting compatible OTel module versions
- `otel-telemetrygen` — generating synthetic OTLP to test pipelines/components

**From `ollygarden/skills`** (OllyGarden's opinionated guides):
- `ollygarden-otel-instrumentation-planning` — deciding what/how to instrument
- `ollygarden-otel-go-setup` — Go SDK setup
- `ollygarden-otel-sdk-setup` — provider/exporter/processor wiring
- `ollygarden-otel-manual-instrumentation` — adding spans/metrics/logs by hand

## AGENTS.md content

- Move the existing `CLAUDE.md` sections verbatim: Overview, Build Commands,
  Architecture (Module Structure, Distribution, Components, key files),
  Configuration.
- Add a top line: "Read `CONTRIBUTING.md` first for project-wide conventions."
- To avoid duplication, the build/test/lint commands and conventions become
  canonical in `CONTRIBUTING.md`; `AGENTS.md` keeps the architecture/component
  map and the Configuration reference, and references CONTRIBUTING.md for the
  shared how-to. (The component architecture detail stays in AGENTS.md as the
  agent's map of the codebase.)
- Add the **Development workflow** section (below).

### Development workflow section (AGENTS.md)

Applies to **feature/behavior changes** (new components, features, behavior
changes). Trivial fixes (typos, comments, tiny localized bugfixes) skip
brainstorm/spec/plan and may go straight to a branch + PR. Dependency PRs use the
existing merge-bot skill.

1. Brainstorm the design → spec in `docs/superpowers/specs/`
   (`superpowers:brainstorming`).
2. Write the implementation plan → `docs/superpowers/plans/`
   (`superpowers:writing-plans`).
3. Create a Linear issue on the **Engineering** team.
4. Branch off `main` using the branch name Linear suggests; branch **before** the
   spec/plan are committed so design docs land on the feature branch.
5. Implement with `superpowers:subagent-driven-development` (per-task TDD +
   spec/quality review, then a final whole-branch review).
6. `make lint` and `make test` must pass.
7. Open a PR referencing the Linear issue (include the `Co-Authored-By` trailer
   on agent commits).
8. Address CodeRabbit comments using `superpowers:receiving-code-review` (verify
   each, don't blindly apply); reply in-thread.
9. A human reviews and squash-merges — the agent never merges.
10. After merge: set the Linear issue to Done and delete the merged branch.

## CLAUDE.md content

Reduce to a short pointer, e.g.:

```markdown
# CLAUDE.md

This project's instructions for coding agents live in [AGENTS.md](AGENTS.md).
Read it first; it also points to CONTRIBUTING.md for project-wide conventions.
```

## Testing / verification

Documentation change — no automated tests. Verification:
- `CLAUDE.md` points to `AGENTS.md`; `AGENTS.md` points to `CONTRIBUTING.md`.
- No convention is documented in two places (build/lint/test commands and
  conventions live only in `CONTRIBUTING.md`).
- The 9 instrumentation skills are listed under the correct two repos.
- Markdown renders (fenced blocks balanced; links resolve to real files).
