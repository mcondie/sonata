# Design notes

Known issues and constraints that are *not* worth a spec of their own yet,
because the right fix belongs inside a future slice. Each note names the
spec that should absorb it. When writing that spec, fold the note in and
delete it here; this file should trend toward empty.

Found during an architecture review of the spec 001 implementation
(2026-08-14). Folded so far: the startup shutdown-race and
`EnsureRunning` extraction (spec 002); `Spawn`'s hand-maintained env
propagation list (spec 004); the idle tracker's busy signal and
SIGKILL orphan reaping (spec 006).

## Fold into the first streaming/long-poll endpoint spec

### Client-wide 10s timeout breaks streaming

`api.NewClient` sets `http.Client.Timeout = 10s`, which caps the entire
request *including body read*. A log-follow or run-wait endpoint dies at
10 seconds regardless of context. Before adding such an endpoint (e.g. a
future `sonata trace --follow`), move timeout control to per-request
contexts; dial and response-header timeouts on the transport can stay.

## Minor, opportunistic

- `api.Server`'s `mu` guards `pid`, which is set once in the constructor
  and never written again — the mutex is dead weight.
- `internal/cli` has no package tests; exit-code mapping and
  `resolveDaemonPID`'s lock-fallback logic are exercised only through E2E.
  Both could run at layer 2 with injected `out`/`errOut` (the `app` struct
  already supports it).
