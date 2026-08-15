# Spec 008 — Sources, trace/graph, and retention

**Status:** planned
**Scope:** the `schedule` source actor (intervals and cron), the
observability CLI the emergent model owes its users (`trace`, `graph`),
and retention (`prune`). Completes the v1 surface of the
[queue/action model](003-queue-action-model.md).

## Goal

Done means a `schedule` action emits on its interval into a pipeline,
`sonata trace <id>` renders the resulting causal tree including a
filtered branch and a retry, `sonata graph` prints the wiring of every
registered action, and `sonata prune` deletes only what is safe.

## In scope

- `github.com/robfig/cron/v3` dependency (add to CLAUDE.md dep list)
- `schedule` actor type: `every:` (Go duration) **or** `cron:`
  (5-field cron expression), mutually exclusive — decided: both in v1
- Workflow validation additions for source actions
- Endpoints/CLI: `trace.show` / `sonata trace <id>`,
  `action.graph` / `sonata graph [--format dot]`,
  `message.prune` / `sonata prune --older-than <dur>` + config TTL
- E2E: the full-pipeline smoke script

## Out of scope

- File-watch and webhook ingress (spec 003 defers them)
- Seconds-granularity or timezone-per-action cron (daemon-local time,
  minute granularity; revisit on demand)
- Log streaming / `trace --follow` (would first need the client
  timeout rework noted in design-notes)
- Archival/export on prune — prune deletes, full stop

---

## `schedule` sources

```yaml
name: poll-inbox
actor: schedule
instructions:
  every: 5m            # XOR with cron:
  # cron: "0 9 * * MON"
  command: ["./check-inbox.sh"]
  timeout: 60s
output: inbox.raw      # required for sources
```

Validation (added to spec 005's table): `schedule` requires empty
`inputs` and a non-empty `output`; exactly one of `every`/`cron`;
`every` ≥ 1s; `cron` must parse (standard 5-field,
`cron.ParseStandard`). Conversely `subprocess` now *requires* ≥1 input
— sources are the only inputless actions.

Execution: the scheduler owns timing on its injected clock (cron next
times computed via `Schedule.Next`; the fake clock drives them in
tests). A firing synthesizes an **ingress-like execution**: run the
command (no stdin), parse stdout as NDJSON exactly as spec 006, append
each line to `output` with a **fresh `trace_id`**, `hops: 0`, but
`origin_action` set — so traces distinguish scheduled ingress from
`sonata send`. Firing records are deliveries with `message_id = NULL`
(relax the NOT NULL in migration 0005) so retries, dead-letter, and
`delivery list` work identically; a source that misses fires while the
daemon is down does **not** back-fill — next fire is computed from
now, and this is documented behavior, not a bug. Concurrency for
sources is forced to 1: a tick never overlaps a still-running previous
tick; a due fire during one is skipped (with a warning log), not
queued. A daemon with a live `schedule` action never idles out —
sources count as busy (unlike parked joins, a source's next fire needs
a running process).

**Sources only fire while a daemon runs — and nothing guarantees one.**
The lifecycle model is autostart-on-*demand*: after `sonata down`, a
crash the lockfile caught, or a reboot, no daemon exists until some
CLI command happens to autostart one, and with no back-fill every fire
in between is silently skipped. This is accepted (Sonata does not
become an init system) but must be *visible*, not discovered:

- `sonata status` on a running daemon reports the count of enabled
  `schedule` actions; when no daemon is running and enabled sources
  exist… it can't know (invariant 1) — which is exactly why the next
  two exist.
- `action.apply` of a `schedule` action returns (and the CLI prints) a
  note that schedules fire only while a daemon runs, recommending
  `sonata daemon` under launchd/systemd for always-on schedules.
- The README's `schedule` section documents the no-daemon/no-fire and
  no-back-fill behavior in the same paragraph that introduces the
  actor type.

## Trace and graph

- `trace.show {trace_id}` walks `messages.origin_message_id` +
  deliveries, returning the causal tree: each node = message, children
  = deliveries it produced (state, action, attempt) and their output
  messages. The CLI renders an indented tree; `--output json` returns
  the structure raw. Filtered and dead deliveries appear — they are
  the answer to "why didn't my action fire."
- `action.graph {}` derives edges from the actions table alone
  (`input queues → action → output queue`), no message data. Default
  output: readable text adjacency; `--format dot` emits Graphviz.
  Disabled actions render marked, not omitted.

## Prune

`message.prune {older_than}`: in batched `write()` transactions,
delete messages older than the cutoff **only if** every delivery of
that message (and, for join members, every delivery sharing its
`execution_id`) is terminal (`done|filtered|dead|cancelled`), cascading their
deliveries and any expired join residue. Returns counts. Config
`retention` (default 0 = keep forever) runs the same logic on the
scheduler's clock daily. `sonata prune` prints what was deleted;
`--dry-run` counts without deleting.

---

## Test plan

**Layer 1:** source validation table rows; cron/every parsing; "next
fire after missed window" arithmetic; prune eligibility predicate
(non-terminal delivery blocks; join sibling blocks).

**Layer 2:** fake-clock scheduler tests — `every` fires on schedule,
`cron` fires at computed instants, no-overlap rule (slow tick skips
the next fire and logs), source failure retries then dead-letters,
fresh trace per fire; `status` reports the enabled-source count and
`action.apply` of a schedule action returns the daemon-required note;
trace.show on a constructed history (pipeline
with a filtered branch, a retry, and a join) matches the expected
tree; graph output for a registered mesh, both formats; prune deletes
the eligible half of a constructed dataset and refuses the rest, under
concurrent appends with `-race`.

**E2E — one script, the capstone:** `pipeline_smoke.txtar` — apply a
`schedule` source (1s interval) feeding a subprocess action; autostart
via `sonata action apply`; poll `delivery list --output json` until
the downstream action has a `done` delivery; `sonata trace` on the
produced trace id shows source → action; `sonata down`. This is the
one script that proves the whole model against the real binary.

## Done when

- [ ] Fake-clock tests for `every`, `cron`, and no-overlap green
- [ ] `trace` output correct for the filtered/retry/join fixture
- [ ] `graph --format dot` parses under `dot -Tsvg` (checked in test
      only if `dot` is on PATH, skipped otherwise)
- [ ] Prune safety tests green under `-race`
- [ ] `pipeline_smoke.txtar` passes in `make test-integ`
- [ ] CLAUDE.md dependency list updated with `robfig/cron/v3`; its
      old DAG-era wording for `internal/scheduler`/`internal/workflow`
      updated to match the queue/action model
