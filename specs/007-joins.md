# Spec 007 — Joins

**Status:** planned
**Scope:** activate `correlate_on` actions: the join buffer, matching,
TTL expiry, and multi-message dispatch. Definitions already parse and
validate (spec 005); the scheduler stops skipping them.

## Goal

Done means the canonical join works in-process: messages on
`invoices.approved` and `shipments.confirmed` sharing an `order_id`
fire `close-order` exactly once with both envelopes on stdin; an
unmatched partner dead-letters when `join_timeout` (fake clock)
expires; and a duplicate key on one input dead-letters as a duplicate.

## In scope

- Migration `0004`: `join_waits`
- Scheduler: correlation-key evaluation, match/buffer logic, expiry
  sweep on the injected clock
- Executor dispatch of a multi-message input set (format already fixed
  by spec 006 — one envelope line per message, in `inputs` order)
- Trace propagation for joins (earliest input's `trace_id`; others
  recorded in the output messages' headers as `joined_traces`)
- `delivery.show` surfacing join context (which messages matched)

## Out of scope

- Latest-wins or windowed join semantics (spec 003 defers them;
  earliest-wins is the rule until a use case demands more)
- Firing on partial input at expiry (rejected in spec 003)
- Joins across more than one *firing* per key — a key fires once;
  a second full set under the same key is a new independent match

---

## Semantics (fixing what spec 003 left abstract)

A join action has N ≥ 2 inputs, each with `correlate_on`. Message
processing for a join replaces the eager per-message delivery of
spec 006 with buffer-then-match, still materialized at append time and
resolved inside scheduler transactions:

1. **Arrival.** A message on a join input gets a `pending` delivery as
   usual. When the scheduler claims it, it evaluates the input's
   filter first (filtered messages never enter the buffer), then
   `correlate_on` → key (eval error → `dead`, as with filters).
2. **Buffer or match.** In one `write()`: look up `join_waits` for
   (action, key). If every *other* input already holds a wait row →
   consume all rows, mark all participating deliveries `claimed` as a
   unit, and dispatch one execution whose input set is the matched
   messages ordered by the action's `inputs` list. Otherwise insert a
   wait row `(action_name, correlation_key, input_queue, message_id,
   expires_at = now + join_timeout)` and park the delivery in a new
   state: **`waiting`**.
3. **Duplicates.** A second message from the *same* input with a key
   that already has a wait row from that input: earliest wins; the
   newcomer's delivery goes `dead` with error `duplicate join key`
   (spec 003's v1 rule).
4. **Expiry.** The scheduler's clock drives a sweep (piggybacked on the
   backoff timer): wait rows past `expires_at` are deleted and their
   deliveries moved `waiting → dead`, error `join timeout`, in one
   transaction.
5. **Completion.** Success/failure applies to all participating
   deliveries as a unit — one subprocess ran for all of them. Retry
   re-fires with the same matched set (the wait rows are already
   consumed; the match itself is durable via the claimed deliveries'
   shared `execution_id`).
6. **Replay.** Two kinds of dead join delivery, two behaviors, told
   apart by `execution_id`:
   - **Timeout dead** (no `execution_id` — it never matched):
     `delivery.replay` re-enters that message into buffer/match as if
     it had just arrived. Its partners are still missing, so this only
     helps once they can arrive; otherwise it parks and times out
     again.
   - **Execution-failure dead** (`execution_id` set — the set matched,
     ran, and exhausted `max_attempts`): the partners are dead *rows*,
     not future messages, so buffer re-entry would wait forever.
     Replaying **any** member resets the *entire matched set* to
     re-fire as a unit — same messages, attempt 0, executed under the
     current action version (the point of replaying after a fix).
     The response lists the peer deliveries it reset.

Schema:

```sql
-- 0004_joins.sql
CREATE TABLE join_waits (
    action_name     TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    input_queue     TEXT NOT NULL,
    message_id      TEXT NOT NULL REFERENCES messages(id),
    expires_at      TEXT NOT NULL,
    PRIMARY KEY (action_name, correlation_key, input_queue)
);
ALTER TABLE deliveries ADD COLUMN execution_id TEXT;  -- groups a matched set
CREATE INDEX join_waits_expiry ON join_waits(expires_at);
```

The `deliveries.state` set gains `waiting`. Spec 006's
disable-cancels rule extends to it: disabling a join action moves its
`waiting` deliveries to `cancelled` and deletes their wait rows in the
same transaction — a disabled join must not hold a parked buffer that
blocks prune. The idle-busy rule from
spec 006 counts `waiting` as **not** busy (a daemon parked on a 24h
join must be allowed to idle out — the wait survives restart in the
DB, and expiry is re-evaluated against wall clock on the next start).

Version pinning nuance: the matched set executes under the version
current when the *match completes* (the final claim), not when the
first member arrived — one execution, one version.

---

## Test plan

**Layer 1:** correlation-key coercion (string, int, eval error);
expiry arithmetic.

**Layer 2 (fake clock + fake executor throughout):** two-input match
in either arrival order; three-input match; filter-rejected message
never buffers; duplicate-key dead-letter; timeout → both `dead` with
`join timeout` (only the waiting side, verify the never-arrived side
produces nothing); replay of a timed-out member re-matches when the
partner then arrives; replay of an execution-failure member resets and
re-fires the whole matched set under the current version; disabling a
join action cancels `waiting` members and clears their wait rows;
retry re-fires the same matched set; version
pinned at match time while an apply lands between first arrival and
match; contention test appending correlated pairs from several
goroutines under `-race`, asserting exactly one execution per key.
Restart durability at layer 2: build the store state, close, reopen,
run a scheduler — parked waits still match/expire correctly.

**E2E:** none — no new process boundary.

## Done when

- [ ] Canonical two-input join green in-process under `-race`
- [ ] Timeout, duplicate, and replay-rematch tests green
- [ ] Replay of a failed execution resets the whole set; disable
      cancels `waiting` deliveries and their wait rows
- [ ] Restart-durability test green
- [ ] Idle timeout treats `waiting` as idle
- [ ] `delivery show` on a join member names its `execution_id` peers
