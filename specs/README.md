# Specs

Implementation specs, numbered in the order they're intended to be built. Each
one is scoped to a slice that can be built and verified on its own.

| # | Spec | Status |
| --- | --- | --- |
| 001 | [Daemon lifecycle](001-daemon-lifecycle.md) — `up`/`status`/`down`/`daemon`, socket, locking, E2E harness | implemented |
| 002 | [Startup race + `EnsureRunning`](002-startup-race-and-ensure-running.md) — tolerate dying predecessor, extract autostart primitive | implemented |
| 003 | [Queue/action model](003-queue-action-model.md) — data model + execution semantics design record; workflows emerge from actions wired by queues | design accepted |
| 004 | [Store + message plane](004-store-and-message-plane.md) — SQLite bring-up, `send`/list/show, autostart-on-demand | implemented |
| 005 | [Action definitions](005-action-definitions.md) — parsing, CEL, validation, versioned `action apply` | planned |
| 006 | [Scheduler + executor](006-scheduler-and-executor.md) — deliveries, retries, dead-letter, transactional outbox, subprocess actor | planned |
| 007 | [Joins](007-joins.md) — correlation buffer, matching, TTL expiry | planned |
| 008 | [Sources + observability](008-sources-and-observability.md) — `schedule` actor, `trace`/`graph`, prune | planned |

[design-notes.md](design-notes.md) holds known issues whose fix belongs
inside a future slice, each tagged with the spec that should absorb it.

## Conventions

- A spec states its **acceptance criterion** as something runnable, not a
  description. "Spec 001 is done when these four scripts pass" beats "the daemon
  starts reliably."
- **Out of scope** is a required section. Listing what a slice deliberately
  does *not* build is what keeps it a slice.
- Specs record decisions and their reasons. When a decision changes, edit the
  spec and say why in the commit message rather than leaving it stale.
- Cross-cutting rules that outlive a single slice belong in `CLAUDE.md`, not
  here. A spec may restate one for local context, but `CLAUDE.md` is the source
  of truth.
