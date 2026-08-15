# Spec 005 — Action definitions, validation, and registration

**Status:** planned
**Scope:** authoring and registering actions — parsing (YAML + JSON into
one struct), CEL compilation, validation, the versioned `actions` table,
and the `action.*` endpoints/CLI. Registered actions are inert: nothing
executes until spec 006.

## Goal

Done means an action YAML round-trips: `sonata action apply -f x.yaml`
validates it, stores version 1, re-applying an edited file stores
version 2, and `sonata action show` prints either. Every invalid
definition class in the validation table below is rejected with a
one-line actionable error.

## In scope

- `github.com/google/cel-go` dependency (add to CLAUDE.md dep list)
- `internal/workflow`: the `Action` struct, YAML/JSON decoding,
  validation, CEL compile (compiled programs cached per (name, version))
- Migration `0002`: `actions` table
- Endpoints: `action.apply`, `action.list`, `action.show`,
  `action.enable`, `action.disable`
- CLI: `sonata action apply -f <file>` (or JSON on stdin with `-f -`),
  `action list`, `action show <name> [--version n]`,
  `action enable|disable <name>`

## Out of scope

- Execution, deliveries, the scheduler — registration only
- `schedule`/source actors (slice 008); this slice validates
  `actor: subprocess` only and rejects unknown actor types, so adding
  a type later is additive
- Join *matching* (slice 007) — but join **fields** are parsed and
  validated now, so definitions are forward-compatible
- Deleting actions (disable covers the need; hard delete would orphan
  delivery history)

---

## Definition shape

Decided in spec 003; restated as the concrete struct contract:

```yaml
name: close-order                # required; [a-z0-9-], unique per version
inputs:                          # required unless actor is a source type
  - queue: invoices.approved     # required
    filter: 'payload.total > 0'  # optional CEL
    correlate_on: 'payload.order_id'  # optional CEL; join marker
  - queue: shipments.confirmed
    correlate_on: 'payload.order_id'
actor: subprocess                # required
instructions:                    # actor-specific, opaque to this table
  command: ["./close.sh"]        # required for subprocess, argv form
  timeout: 300s                  # optional, default from config
output: orders.closed            # optional; omitted = terminal action
concurrency: 4                   # optional, default 1
max_attempts: 3                  # optional, default from config
join_timeout: 24h                # only valid when joining
```

Validation (all in `internal/workflow`, shared by YAML and JSON paths
per the CLAUDE.md rule — never in the YAML parser alone):

| Reject when | Error mentions |
| --- | --- |
| missing/invalid `name`, `actor`, `inputs`, `instructions.command` | the field |
| duplicate queue within `inputs` | the queue |
| `correlate_on` on some but not all inputs | join must be all-or-nothing |
| `correlate_on` with a single input | joins need ≥2 inputs |
| `join_timeout` without `correlate_on` | the dependency |
| any CEL expression that fails to compile | expression + CEL error |
| unknown `actor` type | the type and the known list |
| `concurrency` or `max_attempts` < 1 | the field |
| `input queue == output` queue | self-loop needs an intermediate queue |

The self-loop rule is deliberate: direct self-cycles are almost always
a typo, while A→B→A remains legal per spec 003's hop-cap decision.

CEL environment: `payload` (dyn), `headers` (map), `queue` (string),
`trace_id` (string). Filters must evaluate to bool; `correlate_on` to
a string or int (coerced to string as the correlation key) — checked
at compile time where CEL's type-checker allows, at eval time otherwise.

## Registration model

Apply-into-DB (decided): the CLI parses and the *daemon* re-validates
(the daemon must never trust the client — ad-hoc JSON callers hit the
endpoint directly). The DB is the source of truth for execution; files
are authoring artifacts.

```sql
-- 0002_actions.sql
CREATE TABLE actions (
    name            TEXT NOT NULL,
    version         INTEGER NOT NULL,
    definition      TEXT NOT NULL,   -- canonical JSON
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (name, version)
);
```

- `action.apply` inserts `max(version)+1` for the name inside one
  `write()` transaction. Applying a definition byte-identical (after
  canonicalization) to the current version is a no-op that reports the
  existing version — re-applying a directory of files must be
  idempotent.
- `enabled` lives on the *name* (latest version's flag governs; the
  enable/disable endpoints flip it by writing it to the current
  version row). **Apply carries the flag forward:** the new version
  row inherits the current version's `enabled` value, never the column
  default — applying an edit to a disabled action must not silently
  re-enable it. Re-enabling is only ever the explicit
  `action.enable` call. Disabled actions accrue no new deliveries once
  the scheduler exists, and *disable cancels outstanding work*: the
  action's non-terminal deliveries move to the terminal `cancelled`
  state (mechanics owned by [spec 006](006-scheduler-and-executor.md);
  in this slice, before the scheduler exists, the flag flip is the
  whole behavior).

## API shape

- `POST /v1/action.apply` `{definition}` →
  `{name, version, changed: bool}`
- `POST /v1/action.list` `{}` → current version + enabled per name
- `POST /v1/action.show` `{name, version?}` → the stored definition
- `POST /v1/action.enable|disable` `{name}` → `{name, enabled}`

Validation failures map to a 400 `invalid_action` error whose message
is the one-line validation error (actionable-in-one-line rule).

---

## Test plan

**Layer 1 (bulk of the slice):** table-driven validation tests covering
every row of the reject table plus accept cases (minimal action, full
join, terminal action with no output); YAML and JSON decoding into
identical structs; CEL compile-and-eval round trips including the
bool/string type checks; canonicalization stability (apply → serialize
→ apply is byte-identical).

**Layer 2:** endpoint tests through client → server → store: apply
creates v1, edited apply creates v2, identical apply reports
`changed: false`, show retrieves both versions, enable/disable
round-trips, applying a new version to a *disabled* action leaves it
disabled, invalid definition returns the `invalid_action` shape.
Concurrent applies of the same name from several goroutines under
`-race`: versions come out dense and distinct (the `write()` serializer
is what's under test).

**E2E:** none. Nothing here crosses a process boundary; autostart is
already covered by spec 004's scripts.

## Done when

- [ ] Every reject-table row has a failing-input test and a distinct error
- [ ] Concurrent-apply test green under `-race`
- [ ] `sonata action apply/list/show/enable/disable` honor `--output json`
- [ ] Re-applying an unchanged file is a no-op (`changed: false`)
- [ ] CLAUDE.md dependency list updated with `cel-go`
