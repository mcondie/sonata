# Spec 006 — Scheduler and subprocess executor

**Status:** planned
**Scope:** the execution half of the model for single-input actions:
deliveries, the single-goroutine scheduler, the subprocess actor, the
transactional outbox, retry/dead-letter, and the `delivery.*`
endpoints. Absorbs two design-notes items (idle-busy signal, orphan
reaping). Joins and sources stay out.

## Goal

Done means a two-action pipeline works end to end in-process: send a
message → action A's subprocess runs → its NDJSON output lands on A's
output queue → action B fires on it → `sonata delivery list` shows the
whole story including a filtered and a dead delivery, and killing a
daemon mid-run leaves no orphan process after the next daemon starts.

## In scope

- Migration `0003`: `deliveries`
- Delivery materialization on message append
- `internal/scheduler`: claim loop, filter eval, hop cap, backoff
  timing (injected clock), concurrency limits
- `internal/executor`: subprocess actor — process groups, stdin/stdout
  contract, timeout, output capture
- Transactional outbox: complete + emit in one `write()`
- Endpoints/CLI: `delivery.list`, `delivery.show`, `delivery.replay`
- Idle tracker learns "busy"; orphan reaping at daemon startup
  (both folded from design-notes — delete them there in this PR)
- Config: `max_hops` (100), `default_max_attempts` (3),
  `default_task_timeout` (5m), backoff base/cap (1s ×2 → 60s, jittered)

## Out of scope

- Joins (slice 007): `correlate_on` actions are accepted by the store
  but the scheduler **skips them** (no deliveries materialize) — they
  activate in 007
- Sources/`schedule` actor, trace/graph rendering, prune (slice 008)
- Log streaming/follow endpoints (the client 10s-timeout design note
  stays until one exists)

---

## Schema

```sql
-- 0003_deliveries.sql
CREATE TABLE deliveries (
    id             TEXT PRIMARY KEY,           -- UUIDv7
    message_id     TEXT NOT NULL REFERENCES messages(id),
    action_name    TEXT NOT NULL,
    action_version INTEGER,                    -- NULL until claimed
    state          TEXT NOT NULL,              -- pending|claimed|done|failed|filtered|dead
    attempt        INTEGER NOT NULL DEFAULT 0,
    not_before     TEXT,                       -- backoff gate, UTC
    pgid           INTEGER,                    -- live subprocess group
    stderr_tail    TEXT,                       -- last 8KiB, terminal states
    error          TEXT,
    claimed_at     TEXT,
    completed_at   TEXT,
    UNIQUE (message_id, action_name)
);
CREATE INDEX deliveries_ready ON deliveries(state, not_before);
CREATE INDEX deliveries_action ON deliveries(action_name, state);
```

**Materialization is eager and forward-only.** Appending a message
inserts, in the same transaction, one `pending` delivery per *enabled,
non-join* action subscribed to that queue. Consequences, both
intended: attaching a new action sees only future messages (new
subscriber starts at the tail — replaying history is a possible later
feature, not silent default behavior), and disabling an action stops
new deliveries without touching in-flight ones. Applying a new action
version does not re-deliver anything.

## Scheduler

One goroutine owns every state transition (invariant 4). Inputs: a
"work may exist" nudge channel (poked by message append and delivery
completion), a backoff timer on the injected clock, and shutdown. Each
cycle, per enabled action with `in-flight < concurrency`, it claims the
oldest eligible `pending` (`not_before` ≤ now) delivery:

1. Pin `action_version` = current version at claim time (spec 003).
2. Enforce the hop cap: message `hops` ≥ `max_hops` → `dead`,
   error `hop limit exceeded`.
3. Evaluate the input's filter (compiled CEL, cached): false →
   `filtered`; evaluation error → `dead` with the CEL error (a broken
   filter is loud, per spec 003).
4. Otherwise mark `claimed`, record `pgid` once the executor reports
   it, and hand off to the worker pool.

Worker results return on a channel; the scheduler (not the worker)
applies the outcome via one `write()`:

- **Success** — parse stdout: each non-empty line must be JSON, else
  the delivery fails (retryable). In a single transaction: delivery →
  `done`, and if the action has an `output` queue, append one message
  per line with `trace_id` inherited, `hops+1`, origin fields set, plus
  the eager pending deliveries those messages imply. This is the
  transactional outbox — a crash cannot consume-without-emitting or
  emit twice.
- **Failure** (non-zero exit, timeout, unparseable stdout, spawn
  error) — `attempt+1`; if `attempt < max_attempts`: state `failed`,
  `not_before = now + min(base·2^(attempt−1), cap)` with ±20% jitter,
  and `failed` deliveries past `not_before` are claimable again.
  Otherwise `dead`. Either way persist `error` and `stderr_tail`.

`delivery.replay` (dead only) resets to `pending`, `attempt` 0, clears
`error` — it will claim under the *current* action version, which is
the point of replaying after a fix.

## Subprocess actor

`internal/executor`, behind a small interface
(`Execute(ctx, spec, input) (stdout []byte, stderrTail []byte, err error)`)
so the scheduler tests can fake it and slice 008 can add actor types.

- Start with `Setpgid`; cancellation and timeout kill the **group**
  (`kill(-pgid, SIGKILL)` after a SIGTERM grace of 5s). Report the pgid
  back for persistence before the process can outlive a daemon crash.
- **stdin**: one NDJSON line per input message — the full envelope
  `{"id":…,"queue":…,"payload":…,"headers":…,"trace_id":…}` (one line
  in this slice; joins will send several, same format — scripts written
  now stay correct). Simple scripts do `jq .payload`.
- **stdout/stderr**: captured to bounded buffers (never inherited,
  invariant 6). Stdout hard cap (config, default 4MiB) — exceeding it
  fails the delivery rather than truncating silently.
- Timeout from `instructions.timeout` or the config default.

## Folded design notes

- **Idle tracker busy signal.** `OnActivity` alone is wrong once runs
  outlive requests. The daemon's idle decision becomes: eligible only
  when in-flight HTTP requests = 0 **and** no delivery is `pending`,
  `failed`-awaiting-retry, or `claimed`. The scheduler exposes this as
  a cheap counter, not a DB poll.
- **Orphan reaping.** On startup, before serving, the daemon reads
  `claimed` deliveries; for each with a `pgid` it kills the group
  (ESRCH is fine), then resets the delivery to `failed` with
  `not_before = now` and error `daemon restarted`. This is why `pgid`
  is in migration 0003 from the start.

## API shape

- `POST /v1/delivery.list` `{action?, state?, message_id?, limit?, before_id?}`
- `POST /v1/delivery.show` `{id}` → full row incl. `stderr_tail`
- `POST /v1/delivery.replay` `{id}` → error `not_dead` unless `dead`

CLI: `sonata delivery list [--action] [--state]`, `show`, `replay`,
all with `--output json`. `delivery list --state dead` *is* the
dead-letter queue.

---

## Test plan

**Layer 1:** backoff schedule (values + jitter bounds), NDJSON stdout
parsing (empty, N lines, garbage line, oversized), hop-cap arithmetic.

**Layer 2 — the bulk.** Fake clock + fake executor scheduler tests:
claim order, concurrency ceiling, filter → `filtered`, CEL eval error →
`dead`, retry schedule → `dead` after max attempts, version pinned at
claim while an apply lands mid-flight, replay-after-fix uses the new
version, outbox atomicity (executor fake that reports success while the
store write is forced to fail once: no half-state). Contention test:
appends from several goroutines while the scheduler drains, under
`-race`. Real-subprocess executor tests: pipeline of two real actions
end to end in-process; a subprocess that spawns a grandchild and
ignores SIGTERM is fully gone after cancel (group kill); timeout kills;
stdout cap enforced. Idle test: daemon with a short idle timeout does
not exit while a slow delivery is claimed, exits after it drains.

**E2E — one script, the process boundary the layers can't reach:**
`orphan_reap.txtar` — start a daemon, send a message driving a
long-running task, `kill -9` the daemon PID, assert the task's process
group is gone after a fresh daemon starts and reaps, and the delivery
shows `failed`/retried rather than stuck `claimed`.

## Done when

- [ ] Two-action pipeline test green in-process under `-race`
- [ ] Outbox atomicity and claim-time version tests green
- [ ] Grandchild-kill and stdout-cap executor tests green
- [ ] `orphan_reap.txtar` passes in `make test-integ`
- [ ] Idle timeout ignores request-quiet-but-busy daemons
- [ ] design-notes.md no longer lists the two folded items
