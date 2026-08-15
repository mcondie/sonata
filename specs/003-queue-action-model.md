# Spec 003 — Queue/action data model and execution semantics

**Status:** design accepted, not yet sliced
**Scope:** the conceptual data model for workflows and the semantics of
executing them. Unlike specs 001–002 this is a *design record*, not an
implementation slice: it fixes the entities, invariants, and decided
trade-offs that the following slices implement. Its "done when" is that
the follow-on specs listed at the bottom can be written against it
without reopening these decisions.

## The model in one paragraph

Sonata does not define workflows end to end. The authored unit is an
**action**: input queue(s), a filter for accepting messages, an actor
that does the work, instructions for that actor, and an output queue.
Workflows are *emergent* — action B consumes action A's output queue,
and the pipeline exists because the queue names line up. This is
choreography, not orchestration: capability is added by attaching new
actions to existing queues, one at a time, without touching or even
knowing about the actions already there.

## Entities

### Queue

A named, durable, ordered stream of messages. A queue is just a name —
no schema, no owner, no declaration step; it exists once something
references it. A queue with no consumers is legal (it is an audit log);
a queue with no producers is legal (it is waiting for one).

### Message

An immutable event. Messages are facts: processing never mutates or
deletes them (retention/pruning is a separate, explicit concern).

| Field | Meaning |
| --- | --- |
| `id` | unique, sortable (time-ordered) |
| `queue` | the queue it was appended to |
| `payload` | JSON |
| `headers` | JSON: system + user metadata |
| `trace_id` | stamped at ingress, propagated verbatim (see Tracing) |
| `hops` | header, incremented per producing action (see Cycles) |
| `origin_action`, `origin_action_version` | who emitted it, `NULL` for ingress |
| `origin_message_id` | causal parent(s), `NULL` for ingress |
| `created_at` | UTC |

### Action

The authored unit. YAML (named, on disk) and JSON (ad-hoc, stdin)
decode into the same struct, per the existing workflow-definition rule.

```yaml
name: summarize-report
inputs:
  - queue: reports.raw
    filter: 'payload.kind == "quarterly"'
actor: subprocess
instructions:
  command: ["./summarize.sh"]
  timeout: 300s
output: reports.summarized
concurrency: 4
```

- `inputs` — one or more queues, each with an optional CEL filter.
  Multiple inputs default to **union** semantics: the action fires on
  any accepted message from any input. Adding `correlate_on` turns the
  inputs into a **join** (below).
- `actor` + `instructions` — the executor type and its config. v1 actor
  types: `subprocess` (the workhorse) and `schedule` (a source, below).
- `output` — a single queue. An execution may emit zero, one, or many
  messages to it, so actions can drop, transform, or split. (Multiple
  named outputs are deferred; an actor that must route can be followed
  by filtered consumers, which composes to the same thing.)
- `concurrency` — max simultaneous executions of this action.

**Actions are versioned.** Editing an action inserts a new immutable
version row; `actions` is (name, version, definition, created_at,
enabled). A delivery records the version it was claimed under and
completes on it — a mid-flight edit affects only future claims. "Which
instructions actually ran" stays answerable forever.

### Delivery

The load-bearing entity: one row per (message × consuming action),
separating the immutable message from its per-consumer processing
state.

```
deliveries(id, message_id, action_name, action_version,
           state, attempt, claimed_at, completed_at, error)
state: pending → claimed → done | failed(retryable) | filtered | dead
```

Deliveries are what give the model:

- **Topic semantics.** Every action subscribed to a queue gets its own
  delivery for every message. Two actions on one queue both process
  everything; attaching a new action can never steal messages from or
  change the behavior of existing ones. Competing consumers is not a
  thing between actions — parallelism *within* an action is the
  `concurrency` setting.
- **Filter visibility.** A message failing an action's filter gets a
  delivery in state `filtered` and the action moves on. The cursor
  always advances — one unmatched message must never wedge a queue —
  but the non-match is recorded, because "why didn't my action fire?"
  is the first debugging question this model produces, and it must be
  queryable, not forensic.
- **Retry and dead-letter.** Actor failure → retry with backoff up to
  the action's attempt limit → `dead`. The dead-letter queue is not a
  queue; it is `deliveries WHERE state='dead'`, listable and replayable
  (`sonata deliveries replay`).

## Decided semantics

### Joins (in scope for v1)

An action whose `inputs` carry a `correlate_on` CEL expression is a
join: it fires once per correlation key, when one accepted message from
*each* input shares that key.

```yaml
inputs:
  - queue: invoices.approved
    correlate_on: 'payload.order_id'
  - queue: shipments.confirmed
    correlate_on: 'payload.order_id'
output: orders.closable
join_timeout: 24h
```

- Partial matches live in a join-buffer table
  (`join_waits(action_name, correlation_key, input_queue, message_id,
  expires_at)`), written and matched inside the scheduler's
  transaction.
- On full match, the actor receives all matched messages as one input
  set and the join_waits rows are consumed.
- **Expiry: TTL → dead-letter.** Each join has a `join_timeout`
  (default 24h). A partial match that expires moves its delivery to
  `dead` with a `join timeout` error — a missing upstream is loud,
  visible, and replayable, never a silent accumulation. Firing with
  partial input was rejected: it forces every join consumer to handle
  nulls.
- If several messages from one input share a key before the sibling
  arrives, the earliest wins and later ones dead-letter as duplicates
  (simplest v1 rule; revisit if a real use case needs latest-wins).

### Cycles: allowed, hop-capped

Nothing prevents A's output feeding B and B's feeding A — and that is a
feature (retry loops, iterative refinement), not a bug. The guard is a
`hops` counter in message headers, incremented every time an actor
emits a message caused by another. Exceeding the cap (config
`max_hops`, default 100) dead-letters the message. A runaway loop dies
loudly at a bounded cost instead of filling the database. Static cycle
rejection was rejected: it kills legitimate loops and makes action
registration order-sensitive.

### Expressions: CEL

Filters and `correlate_on` both use
[CEL](https://github.com/google/cel-go): total, side-effect-free,
non-Turing-complete by design — exactly the safety profile a predicate
evaluated inside the scheduler needs. Environment exposes `payload`,
`headers`, and message metadata. Programs are compiled once per action
version and cached. A filter evaluation error dead-letters the delivery
(a broken filter must be loud). `cel-go` is a new intentional
dependency, pure Go, added to the CLAUDE.md dependency list when the
slice lands.

### Ingress

- **`sonata send <queue>`** with JSON on stdin — the API endpoint
  (`POST /v1/message.send`) stamps a fresh `trace_id`, `hops = 0`, no
  origin. This is the scripted/agent entry point and must round-trip
  cleanly with `--output json`.
- **`schedule` actor type** — a *source* action: no inputs, a cron
  expression in its instructions, fires on schedule, and its actor's
  output enters the output queue as ingress messages (fresh trace).
  This gives the system a heartbeat without new platform surface.
- File-watchers and HTTP webhook listeners are explicitly deferred:
  both add real platform/security surface and both can be emulated
  today by external tools calling `sonata send`.

### Tracing

There is no "run" row — the workflow is emergent, so run-level status
cannot exist a priori. The replacement, non-negotiable:

- Every ingress message gets a fresh `trace_id`; every message an actor
  emits inherits the `trace_id` of the message(s) that caused it (a
  join propagates the *earliest* input's trace and records the others
  in headers).
- `origin_message_id` links child to parent, so `sonata trace <id>`
  reconstructs the actual causal tree — including filtered deliveries,
  retries, and dead-letters — after the fact.
- `sonata graph` renders the *implied* static graph (which actions feed
  which queues) straight from the actions table, restoring the "see the
  whole workflow" view a declared DAG would have given.

### Execution loop

Maps directly onto the existing invariants:

1. The **scheduler** (single goroutine, invariant 4) finds pending
   deliveries — materialized when a message is appended, one per
   subscribed action — evaluates filters, resolves join matches, and
   claims work up to each action's `concurrency`, handing it to the
   worker pool.
2. The **actor** runs (subprocess: matched message(s) on stdin, output
   captured never inherited, process-group kill on cancel). Its stdout
   is parsed as zero or more output messages.
3. Completion is a **transactional outbox**: marking the delivery
   `done` and appending its output messages (with propagated trace,
   incremented hops) happen in one `BEGIN IMMEDIATE` transaction
   through the store's `write()` helper. A crash can never produce
   consumed-but-unemitted or emitted-twice. This is the payoff of one
   embedded SQLite database and should be leaned on hard — it is the
   thing genuinely hard in distributed queue systems and free here.

HTTP handlers only append messages and read state; they never wait on
execution (invariant 5).

### Retention

Messages and deliveries accumulate by design. v1 policy: keep
everything by default; `sonata prune --older-than <d>` deletes
messages (and their deliveries/join residue) past age, refusing to
delete anything with a non-terminal delivery. An optional config TTL
can run the same logic on a timer. Nothing else in the model depends on
this, so it can land in any later slice.

## Schema

The authoritative schemas are the migrations specified in the slices:
`messages` in [spec 004](004-store-and-message-plane.md), `actions` in
[spec 005](005-action-definitions.md), `deliveries` in
[spec 006](006-scheduler-and-executor.md), and `join_waits` (plus the
`waiting` state and `execution_id`) in [spec 007](007-joins.md). This
spec fixes the entities and their meaning; columns, indexes, and the
delivery materialization strategy are decided there.

## Out of scope (deferred, with reasons)

- **Multiple named outputs per action** — filtered consumers compose to
  the same result; add only if a real workflow can't express routing
  that way.
- **File-watch and webhook ingress** — platform/security surface;
  external tools + `sonata send` cover it meanwhile.
- **Latest-wins / windowed join semantics** — earliest-wins +
  dead-letter duplicates until a use case demands more.
- **Actor types beyond `subprocess` and `schedule`** (HTTP-call actors,
  inline expressions) — the actor interface should make these easy, but
  none ship in v1.
- **Cross-machine anything.** The model is local by construction.

## Interaction with existing design notes

Two design-notes items were constrained by this model and have since
been folded into [spec 006](006-scheduler-and-executor.md), per that
file's protocol: the idle tracker's busy signal (in-flight requests +
non-idle deliveries) and orphan reaping after SIGKILL (task pgids
persisted from the first deliveries migration).

## Follow-on slices

Written as specs 004–008, in build order:

1. **[004 — Store + message plane](004-store-and-message-plane.md)** —
   SQLite bring-up, messages schema, `send`/list/show,
   autostart-on-demand.
2. **[005 — Action definitions](005-action-definitions.md)** — parsing,
   CEL compile, validation, versioned registration. No execution.
3. **[006 — Scheduler + executor](006-scheduler-and-executor.md)** —
   single-input actions end to end: deliveries, filters, retries,
   dead-letter, transactional outbox, subprocess actor.
4. **[007 — Joins](007-joins.md)** — `correlate_on`, join buffer, TTL
   expiry, multi-message dispatch.
5. **[008 — Sources + observability](008-sources-and-observability.md)** —
   `schedule` actor (`every:` and `cron:`), `sonata trace`/`graph`,
   prune.

Slice-level decisions settled after this spec: subprocess stdout is
NDJSON (one JSON line = one output payload); registration is
apply-into-DB with the DB as execution's source of truth; `schedule`
sources support both `every:` intervals and 5-field `cron:`
expressions (via `robfig/cron/v3`).
