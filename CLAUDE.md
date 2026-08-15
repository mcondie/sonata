# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

Sonata is a local workflow orchestration system written in Go. Two binaries:

- `sonata` — CLI (Cobra + Viper), short-lived, stateless
- `sonatad` — daemon, long-running, owns the SQLite database and executes runs

CLI instances talk to the daemon over a Unix domain socket using gRPC. See
`README.md` for the user-facing overview.

## Non-negotiable invariants

Violating any of these is a bug, not a style preference.

1. **The daemon is the only process that opens the database.** The CLI must never
   import `internal/store` or touch the `.db` file. If a CLI command needs data,
   add an RPC.
2. **All writes go through a transaction in `internal/store`.** No `db.Exec` from
   scheduler or executor code.
3. **The scheduler is single-goroutine for state transitions.** Task execution
   fans out to workers, but the decision of what to run next happens in one
   place. Do not add locks to work around this — funnel through the scheduler's
   channel instead.
4. **Never block the API handler on task execution.** Handlers enqueue and
   return; the caller polls or streams.
5. **Task output is captured, never inherited.** Executors must not let a
   subprocess write directly to the daemon's stdout/stderr.

## Layout

```
cmd/sonata/          CLI entrypoint and command tree
cmd/sonatad/         daemon entrypoint
internal/api/        gRPC handlers (server) and client wrapper
internal/scheduler/  DAG resolution, run queue, retry/backoff
internal/executor/   subprocess execution, log capture, timeouts
internal/store/      SQLite layer, queries, migrations/
internal/workflow/   definition parsing and validation
internal/config/     Viper setup, defaults, path resolution
proto/               .proto definitions (generated code is checked in)
```

Anything under `internal/` is not a public API — change signatures freely.

## Conventions

**Errors.** Wrap with context using `fmt.Errorf("...: %w", err)`. Sentinel errors
live next to the package that owns them (`store.ErrNotFound`,
`workflow.ErrCycle`). API handlers translate sentinels into gRPC status codes in
one place — `internal/api/errors.go`. Do not build status codes inline.

**Context.** Every function that does I/O takes `ctx context.Context` as the
first parameter. Task execution respects cancellation — a cancelled run must
actually kill its subprocesses (process group kill, not just `cmd.Process.Kill`).

**Logging.** `log/slog` only, structured key/value. Include `run_id` and
`task_id` on anything run-related. The CLI logs to stderr; user-facing output
goes to stdout so it can be piped.

**Config.** Add new settings in `internal/config` with a documented default.
Bind flags to Viper so the flag → env → file → default precedence stays uniform.
Never read `os.Getenv` directly outside that package.

**SQL.** Hand-written SQL in `internal/store`, no ORM. Parameterized queries
always. Schema changes are new numbered migration files — never edit an existing
migration.

**Time.** Store UTC. The store layer takes and returns `time.Time`; formatting
for humans happens in the CLI.

## Commands

```sh
make build        # both binaries into ./bin
make test         # unit tests
make test-integ   # integration tests, spawns a real daemon in a temp dir
make lint         # go vet + golangci-lint
make proto        # regenerate from proto/ (run after any .proto change)
```

Prefer `make test` over bare `go test ./...` — it sets the tags the integration
tests key off.

## Testing expectations

- Store tests use a real SQLite file in `t.TempDir()`, not mocks. SQLite is fast
  enough and mocks hide constraint bugs.
- Scheduler tests inject a fake clock and a fake executor. Do not use
  `time.Sleep` to synchronize — it makes tests flaky under load.
- Anything touching the daemon lifecycle goes in integration tests, which get a
  fresh socket and DB per test.
- New RPCs need a test that exercises the CLI → daemon path end to end.

## Working on this repo

- When adding a command, the pattern is: Cobra command in `cmd/sonata` →
  client method in `internal/api` → handler → store call. Follow an existing
  command (`run show` is a good template) rather than inventing a new shape.
- Changing the workflow YAML format is a breaking change while the format is
  unstable — say so in the PR description and update the README example.
- Don't add dependencies for things the standard library covers. Current
  intentional deps: Cobra, Viper, `modernc.org/sqlite` (cgo-free), gRPC.
- Don't start the daemon against the user's real state directory during
  development. Use `SONATA_DATABASE` and `SONATA_SOCKET` to point at a temp dir.
