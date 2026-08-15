# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

Sonata is a local workflow orchestration system written in Go. **One binary**,
`sonata`, which is both the CLI and the daemon (`sonata daemon`). CLI instances
talk to the daemon over a Unix domain socket using **HTTP/JSON** — no gRPC, no
protobuf, no codegen step.

The daemon owns an embedded SQLite database (`modernc.org/sqlite`, pure Go, no
cgo) and executes runs. See `README.md` for the user-facing overview.

Design decisions already settled — do not relitigate these without being asked:

| Decision | Choice |
| --- | --- |
| Transport | HTTP/JSON over Unix socket, shared Go types |
| SQLite driver | `modernc.org/sqlite` on `database/sql` |
| Packaging | One binary, daemon auto-starts on demand |
| Definitions | YAML for named workflows, JSON on stdin for ad-hoc |

## Non-negotiable invariants

Violating any of these is a bug, not a style preference.

1. **The daemon is the only process that opens the database.** The CLI must never
   import `internal/store` or touch the `.db` file. If a CLI command needs data,
   add an endpoint.
2. **Two DB handles, and writes go through the writer.** `internal/store` holds a
   write handle pinned to `SetMaxOpenConns(1)` and a pooled read handle. Never
   widen the writer pool — concurrent CLI instances are the normal case, and a
   multi-connection writer produces `SQLITE_BUSY` under load.
3. **Write transactions use `BEGIN IMMEDIATE`.** A deferred transaction upgrades
   from a read lock on first write; two upgrading at once deadlock in a way
   `busy_timeout` cannot resolve. Go through the store's `write()` helper, which
   handles this — no `db.Exec` from scheduler or executor code.
4. **The scheduler is single-goroutine for state transitions.** Task execution
   fans out to workers, but the decision of what to run next happens in one
   place. Concurrent triggers otherwise race on "is this task ready." Do not add
   locks to work around this — funnel through the scheduler's channel.
5. **Never block an HTTP handler on task execution.** Handlers enqueue and
   return; the caller polls or streams.
6. **Task output is captured, never inherited.** Executors must not let a
   subprocess write to the daemon's stdout/stderr.
7. **Auto-start is guarded by a lockfile.** Several CLI instances can discover a
   dead socket simultaneously; exactly one must win the race to spawn a daemon
   and the rest must wait and retry. The spawned child fully detaches.

## Layout

```
cmd/sonata/          entrypoint; dispatches to CLI or daemon mode
internal/cli/        Cobra command tree, output formatting
internal/api/        types.go (shared), client.go, server.go, errors.go
internal/daemon/     lifecycle, socket, auto-start + locking
internal/scheduler/  DAG resolution, run queue, retry/backoff
internal/executor/   subprocess execution, log capture, timeouts
internal/store/      SQLite layer, queries, migrations/
internal/workflow/   definition parsing (YAML + JSON) and validation
internal/config/     Viper setup, defaults, path resolution
```

Anything under `internal/` is not a public API — change signatures freely.

## Conventions

**API shape.** Endpoints are `POST /v1/<noun>.<verb>` taking and returning JSON.
Request/response types live in `internal/api/types.go` and are shared by client
and server — that shared type *is* the contract, so changing a field changes
both sides at once and the compiler catches the mismatch.

**Adding an endpoint.** The pattern is: types in `internal/api/types.go` →
handler in `server.go` → method in `client.go` → Cobra command in `internal/cli`
→ store call. Follow an existing endpoint (`run.show` is a good template) rather
than inventing a new shape.

**Errors.** Wrap with `fmt.Errorf("...: %w", err)`. Sentinel errors live with the
package that owns them (`store.ErrNotFound`, `workflow.ErrCycle`). Handlers
translate sentinels to HTTP status codes in one place — `internal/api/errors.go`.
Do not write status codes inline. Error responses are
`{"error": {"code": "...", "message": "..."}}` so the CLI can render them and
`--output json` stays parseable.

**Context.** Every function doing I/O takes `ctx context.Context` first. A
cancelled run must actually kill its subprocesses — process *group* kill, not
`cmd.Process.Kill()`, or orphaned grandchildren survive.

**Logging.** `log/slog` only, structured key/value. Include `run_id` and
`task_id` on anything run-related. Daemon logs to stderr; CLI user-facing output
goes to stdout so it can be piped.

**Config.** Add settings in `internal/config` with a documented default. Bind
flags to Viper so flag → env → file → default precedence stays uniform. Never
call `os.Getenv` directly outside that package.

**SQL.** Hand-written SQL in `internal/store`, no ORM. Parameterized queries
always. Schema changes are new numbered migration files — never edit an existing
migration.

**Time.** Store UTC. The store layer takes and returns `time.Time`; formatting
for humans happens in the CLI.

**Workflow definitions.** YAML and JSON decode into the same
`workflow.Workflow`. Validation (cycle detection, unknown `depends_on` targets,
duplicate task IDs) lives in `internal/workflow` and runs on both paths — never
in the YAML parser alone, or ad-hoc submissions skip it.

## Commands

```sh
make build        # ./bin/sonata, with version stamped via -ldflags
make build-all    # cross-compile darwin and linux, amd64 + arm64
make test         # unit tests: go test -race -short ./...
make test-integ   # everything, including daemon-spawning tests
make cover        # coverage.html
make lint         # go vet + golangci-lint
make fmt tidy clean
```

Long tests gate on `testing.Short()` rather than a build tag — anything that
spawns a daemon, opens a socket, or waits on a clock starts with:

```go
if testing.Short() {
    t.Skip("integration test")
}
```

`CGO_ENABLED=0` is exported by the Makefile. Don't override it.

## Testing expectations

- Store tests use a real SQLite file in `t.TempDir()`, not mocks. SQLite is fast
  enough and mocks hide constraint bugs.
- **Concurrency has to be tested, not reasoned about.** Anything touching the
  store or scheduler needs a test that hits it from several goroutines and runs
  under `-race`. The failure modes here only appear under contention.
- Scheduler tests inject a fake clock and a fake executor. Never use
  `time.Sleep` to synchronize — it's flaky under load.
- Daemon lifecycle and auto-start go in integration tests, with a fresh socket
  and DB per test. Auto-start needs a test that races several clients against a
  dead socket and asserts exactly one daemon results.
- New endpoints need a test exercising the CLI → daemon path end to end.

## Working on this repo

- Cross-compilation is a hard requirement (macOS and Linux). Never introduce a
  cgo dependency — it breaks `GOOS=linux go build` and `go install` on machines
  without a C toolchain. This is why the SQLite driver is `modernc.org/sqlite`.
- Concurrent clients are the normal case, not an edge case. A common caller is an
  LLM agent running `sonata` non-interactively, so commands must be scriptable,
  `--output json` must work everywhere, and errors must be actionable in one line.
- Changing the workflow definition format is breaking while the format is
  unstable — say so in the PR description and update the README example.
- Don't add dependencies for what the standard library covers. Current
  intentional deps: Cobra, Viper, `modernc.org/sqlite`, a YAML parser. Transport
  is `net/http`.
- Don't run a dev daemon against the real state directory. Point `SONATA_DATABASE`
  and `SONATA_SOCKET` at a temp dir.
