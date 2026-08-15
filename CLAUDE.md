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

## Testing strategy

Three layers. Put each test at the **lowest layer that can actually catch the
bug** — the common failure mode is pushing scheduler and store behavior up into
E2E because it feels "more realistic," which trades sub-second feedback and
`-race` for a slow, flaky suite.

### Layer 1 — unit

Pure logic, no I/O: cycle detection, config precedence, retry backoff, DAG
readiness. Milliseconds. Most tests live here.

### Layer 2 — in-process integration

Real SQLite in a temp dir and the daemon's HTTP server started **in-process**,
driven through the API client. Covers scheduler, store, executor, and handlers
together without spawning anything. This is where the bulk of daemon coverage
belongs — it gets `-race`, full coverage attribution, and fast feedback.

- Store tests use a real SQLite file, not mocks. SQLite is fast enough and mocks
  hide constraint bugs.
- **Concurrency is tested, not reasoned about.** Anything touching the store or
  scheduler needs a test hitting it from several goroutines under `-race`. These
  failure modes only appear under contention.
- Scheduler tests inject a fake clock and a fake executor. Never `time.Sleep` to
  synchronize — flaky under load.
- New endpoints need a test through the client → handler → store path.

### Layer 3 — E2E against the real binary

Uses [testscript](https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript),
with scripts in `testdata/script/*.txtar`. Reserved for what layer 2
*structurally cannot* reach: process spawn and detach, autostart lockfile races,
signal handling, exit codes, socket file lifecycle, flag parsing. **Keep this to
5–10 scripts.** If a case doesn't involve a real process boundary, it belongs in
layer 2.

```
# testdata/script/daemon_lifecycle.txtar
! exec sonata status
stderr 'no daemon'
exec sonata up
exec sonata status
stdout 'running'
exec sonata down
! exec sonata status
```

Harness rules — each of these exists because of a specific failure mode:

- **Build a real binary onto `PATH` in `Setup`.** Do *not* register `main` via
  testscript's in-process `commands` map. The daemon autostarts by re-execing
  `os.Executable()`, which under the in-process path is the *test binary*, not
  `sonata`. Build once in `TestMain` with `go build -cover` so E2E runs
  contribute coverage.
- **Socket paths go under `/tmp`, not `t.TempDir()`.** `sockaddr_un.sun_path`
  caps at 104 bytes on macOS and 108 on Linux. macOS `t.TempDir()` produces long
  `/var/folders/...` paths that blow the cap and fail with a useless
  `bind: invalid argument`. Use `os.MkdirTemp("/tmp", "sn")` for sockets even if
  everything else uses `t.TempDir()`.
- **Always set `SONATA_SOCKET` and `SONATA_DATABASE`** in the script env. If
  config ever falls back to defaults, a test run kills the developer's real
  daemon.
- **Force-kill in cleanup.** A test failing between `up` and `down` leaks a
  daemon that holds the socket and poisons later runs. Clean up by PID from the
  lockfile.
- **No sleeps for readiness.** `up` blocks until the daemon answers a health
  check (see below), so scripts don't poll.
- Autostart needs a script racing several clients against a dead socket,
  asserting exactly one daemon results.

### Daemon lifecycle contract

Testability forced these, and they're better UX regardless:

- `sonata up` blocks until the daemon answers a health check, then exits 0.
  Non-zero with a diagnostic on timeout. It must never return before the socket
  is accepting connections.
- `sonata status` exits 0 and prints state plus PID when running; exits non-zero
  with `no daemon` on stderr when not. The exit code is the contract — scripts
  branch on it.
- `sonata down` blocks until the process is gone and the socket file is removed.
  Idempotent: exits 0 if nothing was running.
- `sonata daemon` runs in the foreground for systemd/launchd; it never forks.
- The daemon supports `--idle-timeout` so orphaned test daemons self-terminate.

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
  intentional deps: Cobra, Viper, `modernc.org/sqlite`, a YAML parser
  (`go.yaml.in/yaml/v3`), `github.com/google/cel-go` (filter and
  `correlate_on` expressions), and `rogpeppe/go-internal` (test-only, for
  testscript). Transport is `net/http`.
- Don't run a dev daemon against the real state directory. Point `SONATA_DATABASE`
  and `SONATA_SOCKET` at a temp dir.
