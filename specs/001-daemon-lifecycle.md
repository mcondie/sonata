# Spec 001 — Daemon lifecycle vertical slice

**Status:** proposed
**Scope:** the smallest end-to-end slice that makes `sonata up` → `sonata status`
→ `sonata down` work against a real binary, with a passing testscript suite.

## Goal

Stand up the process skeleton — CLI, daemon, socket, lifecycle — with no
workflow functionality behind it. Everything after this spec (store, scheduler,
executor, workflows) plugs into a daemon that already starts, serves, and stops
correctly.

The acceptance criterion is a green run of `e2e/testdata/script/daemon_lifecycle.txtar`
executing the real `sonata` binary by name.

## In scope

- `sonata up`, `sonata status`, `sonata down`, `sonata daemon`
- HTTP/JSON server over a Unix socket, with a single `/v1/health` endpoint
- Daemon spawn, detachment, PID/lock file, socket lifecycle, signal handling
- Config resolution (flags → `SONATA_*` env → file → defaults)
- The testscript harness itself

## Out of scope

Explicitly deferred, and no stubs for them:

- SQLite, `internal/store`, migrations — the daemon holds no state yet
- Scheduler, executor, workflows, `run`/`submit`/`logs`
- Autostart-on-demand (needs a command that wants a daemon; the locking
  primitives it depends on are built here and tested via concurrent `up`)

---

## Files

```
cmd/sonata/main.go                          thin main; version vars for -ldflags
internal/cli/root.go                        Cobra root, global flags, Viper binding
internal/cli/up.go                          sonata up
internal/cli/down.go                        sonata down
internal/cli/status.go                      sonata status
internal/cli/daemon.go                      sonata daemon (foreground)
internal/config/config.go                   Config struct, defaults, resolution
internal/api/types.go                       shared request/response types
internal/api/client.go                      Unix-socket HTTP client
internal/api/server.go                      mux + handlers
internal/api/errors.go                      sentinel → HTTP status mapping
internal/daemon/daemon.go                   Run(): listener, serve, shutdown
internal/daemon/lock.go                     flock helpers, PID file
internal/daemon/spawn.go                    detached child spawn, readiness wait
e2e/e2e_test.go                             TestMain: build binary, run scripts
e2e/testdata/script/daemon_lifecycle.txtar  the acceptance test
e2e/testdata/script/up_idempotent.txtar
e2e/testdata/script/up_concurrent.txtar
e2e/testdata/script/down_idempotent.txtar
```

---

## Command contracts

Exit codes are part of the contract — the E2E scripts branch on them.

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Expected negative outcome (no daemon running) |
| 2 | Usage error (bad flag, bad args) |
| 3 | Operational failure (spawn failed, timeout, socket unusable) |

### `sonata up`

Starts the daemon and **blocks until it is accepting connections**. Never
returns before the socket is live — this is what lets E2E scripts avoid sleeps.

1. Acquire the **start lock** (`<state>/sonata.start.lock`, `flock` LOCK_EX,
   blocking, 10s timeout). Serializes all concurrent `up` attempts.
2. Probe health. If a daemon answers → print `daemon already running (pid N)`,
   exit 0. Idempotent.
3. If the socket file exists but connecting yields `ECONNREFUSED`, it's stale —
   unlink it. Only safe while holding the start lock.
4. Spawn `os.Executable()` with `daemon`, detached (see below).
5. Poll health every 25ms until ready or `--timeout` (default 10s) elapses.
6. On ready: print `daemon started (pid N)`, exit 0.
   On timeout: kill the child, print diagnostics including the child's captured
   stderr, exit 3.
7. Release the start lock.

Flags: `--timeout duration` (default `10s`).

### `sonata status`

Query only — **never** starts a daemon, even when autostart lands later.

- Running: exit 0, one line to stdout:
  `running  pid 4821  uptime 3m12s  version 0.1.0  socket /tmp/sn/sonata.sock`
- Not running: exit **1**, `no daemon running at <socket>` to **stderr**.
- `--output json`: always exit-code-consistent, emits
  `{"running":true,"pid":4821,"uptime_s":192,"version":"0.1.0","socket":"..."}`.

### `sonata down`

Stops the daemon and **blocks until the process is gone and the socket file is
removed**.

1. Read PID from the lock file. If absent or the process is gone → print
   `no daemon running`, exit **0** (idempotent, unlike `status`).
2. `SIGTERM` the PID.
3. Wait for the process to exit and the socket file to disappear, up to
   `--timeout` (default 10s).
4. On timeout: `SIGKILL`, unlink the socket, exit 3 with a warning.
5. Print `daemon stopped (pid N)`, exit 0.

### `sonata daemon`

Runs in the foreground; **never forks**. This is the supervised entrypoint
(systemd/launchd) and the target of the spawn in `up`.

Flags: `--idle-timeout duration` (default `0`, disabled) — exit if no request is
received for this long. Prevents orphaned daemons surviving a failed test run.

---

## Daemon internals

### Locking and PID file

Two files in the state directory:

- **`sonata.lock`** — the running daemon holds `flock(LOCK_EX)` on it for its
  entire lifetime and writes its PID as the contents. `flock` is released
  automatically by the kernel on process death, so there is no stale-lock
  problem: if `flock(LOCK_EX|LOCK_NB)` succeeds, no daemon is alive, full stop.
- **`sonata.start.lock`** — held only during `up`'s spawn sequence, to serialize
  concurrent starts.

A second `sonata daemon` must fail fast: if it cannot take `sonata.lock`
non-blocking, it exits 3 with `another daemon is already running (pid N)`.

Do not infer liveness from the PID file alone — PIDs are recycled. The `flock`
is the source of truth; the PID contents are for `down` to signal.

### Startup sequence

1. Create the state directory (0700).
2. Take `flock` on `sonata.lock`; write PID.
3. Unlink a stale socket file if present, then `net.Listen("unix", socket)`.
4. `chmod` the socket to 0600 — it is an unauthenticated control channel and
   must not be world-writable.
5. Serve HTTP on the listener.
6. Signal readiness (the socket accepting connections *is* the signal; `up`
   polls health).

### Shutdown

On `SIGTERM`/`SIGINT`, or idle timeout:

1. Stop accepting; `http.Server.Shutdown(ctx)` with a 5s drain.
2. Close the listener, unlink the socket file.
3. Release the lock, remove the PID file.
4. Exit 0.

Removing the socket file is required — a leftover file makes the next `up` do
stale-socket detection unnecessarily.

### Detached spawn

```go
cmd := exec.Command(exe, "daemon")
cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}  // new session; survives parent
cmd.Stdin = nil
cmd.Stdout = logFile   // <state>/daemon.log
cmd.Stderr = stderrBuf // captured, surfaced if startup fails
cmd.Env = append(os.Environ(), ...)  // socket/db paths propagate
if err := cmd.Start(); err != nil { ... }
_ = cmd.Process.Release()  // do not Wait; child is not ours to reap
```

`Setsid` puts the child in a new session so it survives the CLI exiting and is
not killed by terminal signals. `Release` detaches it from the parent's process
handling.

Config must propagate to the child via env, not just flags — the child re-resolves
config on its own and must land on the same socket path.

---

## HTTP surface

One endpoint this slice:

```
GET /v1/health  ->  200 {"status":"ok","pid":4821,"version":"0.1.0","started_at":"..."}
```

`GET` is a deliberate exception to the `POST /v1/<noun>.<verb>` convention in
CLAUDE.md: it is a read-only liveness probe with no body, and being curl-able
without `-d` is worth the inconsistency. All subsequent endpoints follow the
POST convention.

Errors use the standard envelope:
`{"error":{"code":"...","message":"..."}}`.

The client dials with:

```go
&http.Client{Transport: &http.Transport{
    DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
        return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
    },
}}
```

Requests use the URL `http://sonata/v1/...` — the host is ignored but must be
syntactically valid.

---

## Config

Keys needed for this slice, resolved flags → `SONATA_*` env → file → defaults:

| Key | Env | Default |
| --- | --- | --- |
| `socket` | `SONATA_SOCKET` | `<state>/sonata.sock` |
| `state_dir` | `SONATA_STATE_DIR` | `$XDG_STATE_HOME/sonata`, else `~/.local/state/sonata` |
| `log_level` | `SONATA_LOG_LEVEL` | `info` |

`database` is resolved and validated but unused until the store lands.

---

## Test plan

### E2E harness (`e2e/e2e_test.go`)

```go
func TestMain(m *testing.M) {
    // Build once, with -cover so E2E contributes coverage.
    // Put the binary's dir on PATH so scripts can `exec sonata`.
}

func TestScripts(t *testing.T) {
    testscript.Run(t, testscript.Params{
        Dir: "testdata/script",
        Setup: func(env *testscript.Env) error {
            // short socket dir (see below), SONATA_* vars, cleanup hook
        },
    })
}
```

Harness requirements, each tied to a specific failure mode:

- **Build a real binary onto `PATH`.** Do not use testscript's in-process
  `commands` map: the daemon spawns via `os.Executable()`, which in-process
  resolves to the *test binary*, not `sonata`.
- **Socket paths under `/tmp`, not `t.TempDir()`.** `sun_path` caps at 104 bytes
  on macOS; long `/var/folders/...` paths fail with `bind: invalid argument`.
  Use `os.MkdirTemp("/tmp", "sn")` per script and set `SONATA_SOCKET` into it.
- **Always set `SONATA_STATE_DIR` and `SONATA_SOCKET`.** A config fallback to
  defaults would let a test kill the developer's real daemon.
- **Cleanup force-kills** by PID from the lock file, so a script failing between
  `up` and `down` doesn't leak a daemon holding the socket.
- Pass `--idle-timeout=60s` to spawned daemons as a backstop.

### Scripts

**`daemon_lifecycle.txtar`** — the acceptance test:

```
! exec sonata status
stderr 'no daemon'

exec sonata up
stdout 'daemon started'

exec sonata status
stdout 'running'
stdout 'pid \d+'

exec sonata down
stdout 'daemon stopped'

! exec sonata status
stderr 'no daemon'
```

**`up_idempotent.txtar`** — second `up` exits 0 with `already running`, and the
PID is unchanged.

**`up_concurrent.txtar`** — several `up` in background via `&`, then `wait`; all
exit 0 and exactly one daemon PID exists. This is the start-lock race, and the
primitive autostart will later depend on.

**`down_idempotent.txtar`** — `down` with no daemon exits 0.

### Unit tests

- Config precedence: flag beats env beats file beats default.
- Stale socket detection: a plain file at the socket path with nothing listening
  is correctly identified and unlinked.
- Lock: two processes, second fails `LOCK_NB`; after the first dies the lock is
  free without cleanup.

---

## Done when

- [ ] `make build` produces `./bin/sonata`
- [ ] `make test` passes (unit, `-race -short`)
- [ ] `make test-integ` passes, including all four scripts
- [ ] `sonata up && sonata status && sonata down` works by hand from a shell
- [ ] `sonata daemon` runs in the foreground and stops cleanly on Ctrl-C
- [ ] No daemon survives a deliberately failed test run
- [ ] `make lint` clean
