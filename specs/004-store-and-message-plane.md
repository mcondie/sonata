# Spec 004 — Store foundation and the message plane

**Status:** implemented
**Scope:** the SQLite store (`internal/store`) and the message half of the
[queue/action model](003-queue-action-model.md): append, read, and trace
messages — no actions, no execution. Also the first data commands, which
makes this the slice that turns on **autostart-on-demand** via spec 002's
`EnsureRunning`.

## Goal

Everything after this slice needs a database and messages in it. Done
means: with no daemon running, `echo '{"a":1}' | sonata send demo`
starts a daemon, appends a message, and prints its id; `sonata message
list --queue demo` shows it; and the store layer's concurrency behavior
is pinned by tests under `-race`.

## In scope

- `modernc.org/sqlite` dependency (add to CLAUDE.md dep list)
- `internal/store`: open/migrate, two handles, `write()` helper
- Migration `0001`: `messages` (queues stay implicit — a queue exists
  iff referenced; revisit only if a later slice needs queue metadata)
- Message IDs: **UUIDv7**, hand-rolled on `crypto/rand` (stdlib only;
  time-ordered so `ORDER BY id` is `ORDER BY created_at, entropy`)
- Endpoints: `message.send`, `message.list`, `message.show`,
  `queue.list` (distinct queue names + counts, derived)
- CLI: `sonata send <queue>` (JSON payload on stdin, `--header k=v`),
  `sonata message list [--queue] [--trace] [--limit]`,
  `sonata message show <id>`, `sonata queue list`
- **Autostart-on-demand**: data commands call `daemon.EnsureRunning`
  before dialing; `status`/`down`/`daemon` still never autostart
- Config env propagation fix (folded from design-notes, see below)

## Out of scope

- Actions, deliveries, filters, execution — nothing consumes messages
- Retention/prune (slice 008); the DB only grows in this slice
- Any streaming endpoint (the client-wide 10s timeout note in
  design-notes stays until one exists)

---

## Store contract

Restating the CLAUDE.md invariants this package embodies: one write
handle at `SetMaxOpenConns(1)`, a pooled read handle, all writes
through `store.write(ctx, func(tx) error)` which issues
`BEGIN IMMEDIATE`. `PRAGMA journal_mode=WAL`, `busy_timeout=5000`,
`foreign_keys=ON` on both handles. Migrations are numbered `.sql`
files embedded via `embed.FS`, applied in a transaction each, tracked
in `schema_migrations`.

```sql
-- 0001_messages.sql
CREATE TABLE messages (
    id                    TEXT PRIMARY KEY,          -- UUIDv7
    queue                 TEXT NOT NULL,
    payload               TEXT NOT NULL,             -- JSON
    headers               TEXT NOT NULL DEFAULT '{}',-- JSON, includes hops
    trace_id              TEXT NOT NULL,
    origin_action         TEXT,
    origin_action_version INTEGER,
    origin_message_id     TEXT REFERENCES messages(id),
    created_at            TEXT NOT NULL              -- RFC3339 UTC
);
CREATE INDEX messages_queue ON messages(queue, id);
CREATE INDEX messages_trace ON messages(trace_id, id);
```

`message.send` stamps: fresh `trace_id` (UUIDv7), `hops: 0` in
headers, NULL origins. Payload must be valid JSON — reject otherwise
with a 400-mapped sentinel (`store` stores what it's given; validation
is the handler's job).

## API shape

Follows the `run.show`-style pattern: types in `internal/api/types.go`,
handler → client → Cobra command → store call.

- `POST /v1/message.send` `{queue, payload, headers}` →
  `{id, trace_id, created_at}`
- `POST /v1/message.list` `{queue?, trace_id?, limit?, before_id?}` →
  `{messages: [...]}` (keyset pagination on id)
- `POST /v1/message.show` `{id}` → the full message
- `POST /v1/queue.list` `{}` → `{queues: [{name, messages}]}`

New sentinel: `store.ErrNotFound` → 404 in `internal/api/errors.go`.
All CLI commands support `--output json`.

## Autostart-on-demand

The client-constructing path in `internal/cli` gains a helper that
runs `daemon.EnsureRunning` (default `EnsureOptions`, idle timeout from
config) before returning a client. `send`, `message *`, and `queue *`
use it; lifecycle commands keep their spec 001 semantics. This is the
feature spec 002 built toward — no changes to `EnsureRunning` are
expected; if one proves necessary, that's a finding worth flagging in
the PR.

## Folded design note — `Spawn` env propagation

This slice makes autostart routine, so the hand-maintained four-var
env list in `Spawn` becomes a live hazard. Fix here: derive the
propagated env from `internal/config` itself (one function returning
`[]string` built from the resolved `Config`), so a config field added
later is forwarded automatically. Delete the note from design-notes.md
in the same PR.

---

## Test plan

**Layer 1:** UUIDv7 generation (format, monotonic ordering across a
burst); payload/header validation.

**Layer 2:** store tests on a real temp-dir SQLite file — migration
idempotency (open twice), append/list/show/queue-list, keyset
pagination; a concurrency test appending from 8 goroutines while
readers list, under `-race`, asserting no `SQLITE_BUSY` surfaces.
Endpoint tests through client → in-process server → store for all four
endpoints, including the not-found and invalid-JSON error shapes.

**E2E:** two scripts.

- `send_autostart.txtar`: no daemon; `sonata send` autostarts, exits 0,
  prints an id; `sonata status` confirms; `message list` round-trips
  through `--output json`.
- `autostart_race.txtar`: several concurrent `sonata send` against a
  dead socket; exactly one daemon results (the racing-clients script
  CLAUDE.md's harness rules call for, deferred until now for lack of a
  data command).

## Done when

- [x] `make test` green including the 8-goroutine store test under `-race`
- [x] Both E2E scripts pass in `make test-integ`
- [x] `sonata send`/`message list|show`/`queue list` all honor
      `--output json`
- [x] CLI still does not import `internal/store` (invariant 1)
- [x] `Spawn` env list derived from `internal/config`; design note deleted
- [x] CLAUDE.md dependency list updated with `modernc.org/sqlite`
      (already listed as a settled decision; verified present)
