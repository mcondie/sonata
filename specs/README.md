# Specs

Implementation specs, numbered in the order they're intended to be built. Each
one is scoped to a slice that can be built and verified on its own.

| # | Spec | Status |
| --- | --- | --- |
| 001 | [Daemon lifecycle](001-daemon-lifecycle.md) — `up`/`status`/`down`/`daemon`, socket, locking, E2E harness | implemented |

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
