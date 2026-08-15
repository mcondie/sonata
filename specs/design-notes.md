# Design notes

Known issues and constraints that are *not* worth a spec of their own yet,
because the right fix belongs inside a future slice. Each note names the
spec that should absorb it. When writing that spec, fold the note in and
delete it here; this file should trend toward empty.

Found during an architecture review of the spec 001 implementation
(2026-08-14). Spec 002 covers the two items promoted to immediate work
(the startup shutdown-race and `EnsureRunning` extraction).

## Fold into the scheduler/executor spec

### Idle timeout must learn what "busy" means

`OnActivity` fires at the top of `ServeHTTP`, so the idle tracker only sees
request *arrival*. Two failure modes once the daemon does real work:

- A daemon executing a long run with no client polling it will idle-timeout
  and die mid-run.
- A long-lived streaming connection (log follow) counts as activity only at
  its first byte.

The tracker needs a busy signal — in-flight request count plus active runs
— not just "last request seen". The scheduler/executor are the components
that know when a run is active, so the hook's shape should be decided in
that spec, not bolted on after.

### SIGKILL escalation orphans task subprocesses

`daemon.Stop` escalates to SIGKILL on the daemon PID when a graceful stop
times out. Once the executor runs tasks in their own process groups (as the
cancellation rules require), a SIGKILL'd daemon leaves those groups running
unwatched. This cannot be fixed at kill time — the pattern is: the daemon
persists task PIDs/pgids in SQLite, and the *next* daemon reaps orphans at
startup. The store schema must account for this from the first migration
that records task execution.

## Fold into the first streaming/long-poll endpoint spec

### Client-wide 10s timeout breaks streaming

`api.NewClient` sets `http.Client.Timeout = 10s`, which caps the entire
request *including body read*. A log-follow or run-wait endpoint dies at
10 seconds regardless of context. Before adding such an endpoint, move
timeout control to per-request contexts; dial and response-header timeouts
on the transport can stay.

## Fold into whichever spec next touches config

### `Spawn`'s env propagation is a hand-maintained list

`Spawn` forwards exactly four settings (`STATE_DIR`, `SOCKET`, `DATABASE`,
`LOG_LEVEL`) to the detached child. Every config field added later must be
remembered there, or the child resolves different config than the parent
validated — and the failure is silent divergence, not an error. Fix by
making propagation derive from one source of truth: either generate the env
list from the `Config` struct in `internal/config`, or propagate the
resolved `--config` file path as well. Do this in the same change that adds
the next config field.

## Minor, opportunistic

- `api.Server`'s `mu` guards `pid`, which is set once in the constructor
  and never written again — the mutex is dead weight.
- `internal/cli` has no package tests; exit-code mapping and
  `resolveDaemonPID`'s lock-fallback logic are exercised only through E2E.
  Both could run at layer 2 with injected `out`/`errOut` (the `app` struct
  already supports it).
