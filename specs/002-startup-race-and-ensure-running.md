# Spec 002 — Startup shutdown-race fix and `EnsureRunning`

**Status:** implemented
**Scope:** two changes to daemon startup, both prerequisites for
autostart-on-demand: tolerate a predecessor daemon that is still shutting
down, and extract `up`'s orchestration into a reusable single-flight
primitive.

## Goal

Autostart (deferred from spec 001) means every data command will spawn a
daemon when the socket is dead. Two defects in the current startup path get
hot the moment that lands:

1. **Shutdown race.** `daemon.Run` takes the daemon lock with a non-blocking
   `Acquire` and exits immediately if it is held. But a stopping daemon
   closes its listener *first* and holds the flock through up to 5s of drain
   (`shutdownGrace`). In that window a client sees `ECONNREFUSED`, concludes
   no daemon exists, and spawns a child — which loses the flock to the dying
   daemon, exits `another daemon is already running`, and strands the caller
   in a full `WaitReady` timeout. A retry one second later would have
   worked. Today this needs a manual `up` racing a `down` or an idle
   timeout; with autostart, "daemon idles out exactly as a client arrives"
   is the common case, not the corner.

2. **The autostart sequence lives in the wrong layer.** The probe →
   start-lock → stale-socket removal → spawn → wait-ready sequence is
   embedded in `app.up` (`internal/cli/up.go`). That sequence *is*
   autostart; leaving it in the CLI means every future data command either
   duplicates it or reaches into `cli`. It belongs in `internal/daemon` as
   one function.

The acceptance criterion is a green `make test-integ` including the two new
tests named in the test plan.

## In scope

- `daemon.Run` waits briefly for the daemon lock instead of failing fast
- `daemon.EnsureRunning`, the single-flight probe-or-spawn primitive
- `sonata up` reimplemented as a thin wrapper over `EnsureRunning`
- A spawned-PID sanity check inside `EnsureRunning` (see contract)

## Out of scope

Explicitly deferred, and no stubs for them:

- Autostart-on-demand itself — no command exists yet that wants a daemon.
  This spec builds the exact function such a command will call, and nothing
  more.
- Any change to `status` (still never starts a daemon), `down`, or the HTTP
  surface.
- The idle-tracker rework and other items in [design-notes.md](design-notes.md).

---

## Change 1 — `Run` waits out a dying predecessor

`daemon.Run` replaces its non-blocking `Acquire` of `sonata.lock` with
`AcquireWait` and a grace of **`shutdownGrace` + 2s** (7s): long enough to
outlast any graceful drain, short enough that a genuinely running daemon
still produces a fast, clear failure.

Semantics preserved from spec 001:

- If the lock is still held when the grace expires, exit 3 with
  `another daemon is already running (pid N)` — a *live, serving* daemon
  holds the lock forever, so waiting on it longer buys nothing.
- Fail-fast for a second `sonata daemon` started by hand is gone by design:
  it now fails after ~7s instead of instantly. That is the correct trade —
  the interactive double-start is rare and still errors clearly; the
  restart race is structural.

`AcquireWait` already exists (`internal/daemon/lock.go`) and polls at 20ms,
so no new locking code is needed.

## Change 2 — `daemon.EnsureRunning`

```go
// EnsureOptions configures EnsureRunning.
type EnsureOptions struct {
    // ReadyTimeout bounds the wait for a spawned daemon to answer health.
    ReadyTimeout time.Duration // default 10s
    // IdleTimeout is passed through to the spawned daemon (0 disables).
    IdleTimeout time.Duration
    // StartLockWait bounds the wait on the start lock while another
    // process runs this same sequence.
    StartLockWait time.Duration // default 10s
}

// EnsureRunning returns a healthy daemon's identity, spawning one if
// nothing is listening. started reports whether this call spawned it.
func EnsureRunning(ctx context.Context, cfg *config.Config, opts EnsureOptions) (h *api.HealthResponse, started bool, err error)
```

The body is `app.up`'s current sequence, moved verbatim into
`internal/daemon`:

1. `MkdirAll` the state dir (0700).
2. `AcquireWait` the **start lock** (`sonata.start.lock`) — serializes
   concurrent ensures, exactly as it serialized concurrent `up`.
3. Probe health. Answer → return `(h, false, nil)`.
   Any error other than `ErrNoDaemon` → fail.
4. Unlink a stale socket file if present (safe under the start lock).
5. `Spawn`, then `WaitReady` up to `ReadyTimeout`.
6. On ready, **verify `h.PID` matches the spawned PID**. A mismatch means
   some other daemon answered despite the start lock — a config or lock-dir
   confusion worth failing loudly on, not printing a wrong PID for.
7. On timeout: `Stop` the spawned PID (don't leak a half-started process),
   surface `LogTail`, fail.

Notes:

- `internal/daemon` already imports `internal/api` (for `WaitReady`), so no
  new dependency edge is created. The CLI keeps ownership of all printing
  and exit codes; `EnsureRunning` returns errors and lets `up` map them.
- The log-tail-on-failure behaviour moves with the sequence: the error from
  a failed spawn wraps enough context that `up` can still print the tail.
  Simplest shape: `EnsureRunning` returns a dedicated error type carrying
  the tail, or the CLI calls `LogTail` itself on failure — either is fine;
  pick one and keep the E2E stderr assertions passing.

`sonata up` becomes:

```go
h, started, err := daemon.EnsureRunning(ctx, a.cfg, daemon.EnsureOptions{
    ReadyTimeout: timeout,
    IdleTimeout:  idleTimeout,
})
// started == false → "daemon already running (pid N)"
// started == true  → "daemon started (pid N)"
```

Flags, output lines, and exit codes are unchanged — the spec 001 command
contract for `up` still holds word for word, and the existing E2E scripts
are the regression test for that.

---

## Test plan

### Layer 2 (in-process) — the shutdown race

`internal/daemon`: `TestRunWaitsForDyingPredecessor`. Start `Run` in a
goroutine; once healthy, hold an open request or otherwise ensure it is
inside its drain window, cancel its context, and *immediately* call `Run`
again with the same config. The second `Run` must come up healthy — not
error with "already running". Guard with `testing.Short()`; it spawns no
processes but waits on real shutdown.

Also: `TestRunFailsWhenLockHeldBeyondGrace` — a held lock that never
releases still produces the "already running" error, after the grace.

### Layer 2 — `EnsureRunning`

`TestEnsureRunningIdempotent`: against a daemon already serving in-process,
`EnsureRunning` returns `started == false` and the right PID without
spawning. (The spawn path itself crosses a process boundary and stays in
E2E.)

### E2E

- All five spec 001 scripts pass unchanged — they are the contract that
  `up`'s behaviour survived the extraction.
- `up_concurrent.txtar` is now also exercising `EnsureRunning`'s
  single-flight property, since `up` is a wrapper.

---

## Done when

- [ ] `TestRunWaitsForDyingPredecessor` passes under `-race`
- [ ] `TestRunFailsWhenLockHeldBeyondGrace` passes
- [ ] `TestEnsureRunningIdempotent` passes
- [ ] `internal/cli/up.go` contains no probe/spawn/lock logic — only flag
      handling, the `EnsureRunning` call, and output
- [ ] `make test-integ` green, all spec 001 scripts unmodified
- [ ] `go vet` and `gofmt` clean
