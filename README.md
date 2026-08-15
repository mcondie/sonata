# Sonata

Local workflow orchestration. A single Go binary is both a CLI and a daemon: the
daemon owns state in an embedded SQLite database and executes work, while any
number of CLI instances — yours, a script's, an LLM agent's — connect to it
concurrently to define, trigger, and inspect that work.

Everything runs on your machine. No cluster, no broker, no network dependency.

> **Early development.** The message and action planes are built; execution is
> not. See [Status](#status) for exactly what runs today.

## Why

Existing orchestrators (Airflow, Temporal, Prefect) assume a server, a message
broker, and a database you have to operate. Sonata targets durable, resumable,
event-driven work on one machine — build pipelines, data pulls, local ML jobs,
agent-driven work — without the operational surface.

## The model

Sonata does not define workflows end to end. The authored unit is an **action**:
input queue(s), a filter for accepting messages, an actor that does the work,
and an output queue. Workflows are *emergent* — action B consumes action A's
output queue, and the pipeline exists because the queue names line up.

```
  sonata send                                     emits
       │                                            │
       ▼                                            ▼
  reports.raw  ──▶  summarize-report  ──▶  reports.summarized  ──▶  publish-summary
    (queue)        (action, filtered)            (queue)               (action)
```

This is choreography, not orchestration: you add capability by attaching a new
action to an existing queue, one at a time, without touching — or even knowing
about — the actions already there.

| Entity | What it is |
| --- | --- |
| **Queue** | A named, durable, ordered stream. Just a name: no schema, no declaration step; it exists once something references it. |
| **Message** | An immutable fact — JSON payload, headers, a `trace_id` stamped at ingress, and causal links to whatever produced it. Processing never mutates or deletes one. |
| **Action** | The authored unit: inputs, actor, instructions, output. Versioned — editing one inserts a new immutable version. |
| **Delivery** | One row per (message × consuming action), holding the processing state that the shared message must not carry. *(lands with the scheduler, spec 006)* |

Deliveries are what give the model **topic semantics** (every action subscribed
to a queue gets its own copy of every message, so attaching an action can never
steal work from an existing one), **filter visibility** (a message failing a
filter is recorded as `filtered`, because "why didn't my action fire?" must be
queryable rather than forensic), and a **dead-letter queue** that is not a queue
but a query: `deliveries WHERE state = 'dead'`.

Cycles are allowed and hop-capped rather than statically rejected — retry loops
and iterative refinement are features, and a runaway loop dies loudly at bounded
cost. The full design record is
[`specs/003-queue-action-model.md`](specs/003-queue-action-model.md).

## Architecture

```
┌──────────┐   ┌──────────┐   ┌──────────┐
│ sonata   │   │ sonata   │   │ sonata   │      CLI instances
│ send     │   │ status   │   │ action   │      (concurrent, short-lived)
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    │  HTTP/JSON over a Unix domain socket
                    ▼
            ┌───────────────┐
            │ sonata daemon │  single process, sole DB writer
            │  ├ HTTP server│
            │  ├ scheduler  │  (not built yet — spec 006)
            │  └ executors  │  (not built yet — spec 006)
            └───────┬───────┘
                    │
                    ▼
            ┌───────────────┐
            │ sonata.db     │  SQLite (WAL)
            └───────────────┘
```

**Key constraint:** the daemon is the only process that writes to the database.
CLI instances never open the DB file. This puts all concurrency handling in one
place and keeps the client a thin HTTP wrapper.

Transport is plain HTTP/JSON over the socket, with request and response types in
a shared Go package. No codegen step, and the API is debuggable directly:

```sh
curl --unix-socket ~/.local/state/sonata/sonata.sock \
  -d '{"queue":"reports.raw"}' http://x/v1/message.list
```

### Components

| Component | Responsibility |
| --- | --- |
| `cmd/sonata` | Entrypoint for both CLI and daemon modes |
| `internal/cli` | Cobra command tree, Viper config, output formatting |
| `internal/api` | HTTP handlers, client wrapper, shared request/response types |
| `internal/daemon` | Process lifecycle, socket setup, auto-start and locking |
| `internal/store` | SQLite access layer, migrations, transaction boundaries |
| `internal/workflow` | Action parsing (YAML + JSON), validation, CEL compilation |
| `internal/scheduler` | Delivery materialization, filters, retry and backoff *(planned)* |
| `internal/executor` | Actor execution (subprocess), output capture, timeouts *(planned)* |

## Installation

Requires Go 1.26+. One binary, no C toolchain — the SQLite driver is pure Go, so
cross-compiling is a single env var.

```sh
go install github.com/mcondie/sonata/cmd/sonata@latest
```

From a clone:

```sh
make build                        # ./bin/sonata
make build-all                    # darwin/arm64, linux/amd64, linux/arm64
GOOS=linux GOARCH=arm64 go build ./cmd/sonata
```

## Quick start

There's no setup step. The first command that needs the daemon starts it:

```sh
$ echo '{"kind":"quarterly","total":42}' | sonata send reports.raw
sonata: daemon autostarted
0199a7c4-8f13-7c2a-9d61-3f2b7e5a1c04
```

Or manage it explicitly:

```sh
sonata up                  # start; blocks until the daemon is accepting connections
sonata status              # state and PID; non-zero exit if not running
sonata down                # stop; blocks until the socket is gone
```

`sonata daemon` runs it in the foreground instead, for systemd or launchd.

### Sending messages

Ingress is JSON on stdin. The daemon stamps a fresh `trace_id` and `hops: 0`;
your `--header` values ride along beside them:

```sh
echo '{"kind":"quarterly","total":42}' | sonata send reports.raw
echo '{"kind":"monthly","total":7}'    | sonata send reports.raw --header source=cron
```

Queues are not declared. `reports.raw` exists because that command named it.

### Defining actions

An action is YAML you keep in git. `actions/summarize.yaml`:

```yaml
name: summarize-report
inputs:
  - queue: reports.raw
    filter: 'payload.kind == "quarterly"'
actor: subprocess
instructions:
  command: ["./scripts/summarize.sh"]
  timeout: 300s
output: reports.summarized
concurrency: 4
max_attempts: 3
```

```sh
sonata action apply -f actions/summarize.yaml
```

Apply parses, validates, and stores the definition as the next version.
Re-applying an unchanged file is a no-op, so applying a whole directory
repeatedly is safe:

```sh
$ sonata action apply -f actions/summarize.yaml
summarize-report version 1
$ sonata action apply -f actions/summarize.yaml
summarize-report unchanged (version 1)
```

Editing the file and re-applying stores version 2; version 1 stays readable
forever, so "which instructions actually ran" remains answerable. A new version
inherits the current enabled flag — editing a disabled action never silently
re-enables it.

Ad-hoc callers can skip the file and pipe JSON instead. YAML and JSON decode
into the same struct and run through the same validation:

```sh
echo '{"name":"publish-summary","inputs":[{"queue":"reports.summarized"}],
       "actor":"subprocess","instructions":{"command":["./scripts/publish.sh"]}}' \
  | sonata action apply -f -
```

`filter` and `correlate_on` are [CEL](https://github.com/google/cel-go)
expressions over `payload`, `headers`, `queue`, and `trace_id`. They compile at
registration, so a broken expression is a rejected apply rather than a runtime
surprise:

```sh
$ sonata action apply -f actions/broken.yaml
sonata: actions/broken.yaml: action summarize-report: inputs[0].filter "payload.kind ==": 1:16: Syntax error: …
```

An action with `correlate_on` on every input is a **join** — it fires once per
correlation key, when an accepted message from each input shares that key:

```yaml
name: close-order
inputs:
  - queue: invoices.approved
    correlate_on: 'payload.order_id'
  - queue: shipments.confirmed
    correlate_on: 'payload.order_id'
actor: subprocess
instructions:
  command: ["./scripts/close.sh"]
output: orders.closed
join_timeout: 24h        # a partial match that expires dead-letters, loudly
```

Joins are all-or-nothing: `correlate_on` on some inputs but not others is a
rejected definition, not a half-configured action. Join definitions validate and
version today; matching lands with slice 007.

### Inspecting

```sh
sonata action list
sonata action show summarize-report            # current version
sonata action show summarize-report --version 1
sonata action disable summarize-report         # stop accruing work
sonata action enable summarize-report

sonata queue list
sonata message list --queue reports.raw --limit 20
sonata message list --trace 0199a7c4-…         # everything in one causal trace
sonata message show 0199a7c4-…
```

Every command takes `--output json` for scripting — a common caller is an agent
running `sonata` non-interactively, so every command is scriptable and every
error is one actionable line.

## Command reference

| Command | Description |
| --- | --- |
| `sonata send <queue>` | Append a JSON message from stdin to a queue |
| `sonata action apply -f <file>` | Validate and register a definition as a new version (`-f -` for JSON on stdin) |
| `sonata action list` | List actions with their current version |
| `sonata action show <name>` | Show a stored definition (`--version n` for an old one) |
| `sonata action enable\|disable <name>` | Flip whether an action accrues work |
| `sonata message list` | List messages, newest first (`--queue`, `--trace`, `--limit`, `--before`) |
| `sonata message show <id>` | Show one message in full |
| `sonata queue list` | List queues and their message counts |
| `sonata up` | Start the daemon, waiting until it's ready |
| `sonata status` | Daemon state and PID (non-zero exit if not running) |
| `sonata down` | Stop the daemon, waiting until it's gone |
| `sonata daemon` | Run the daemon in the foreground (systemd/launchd) |

Global flags: `--config`, `--state-dir`, `--socket`, `--database`, `--log-level`,
`--output json|table`, `--verbose`. `sonata daemon` adds `--idle-timeout`.

Exit codes are part of the contract, so scripts can branch on them: `0` success,
`1` no daemon, `2` bad flag or argument (including an invalid definition), `3`
operational failure.

## Configuration

Viper resolves configuration in this precedence order:

1. Command-line flags
2. Environment variables prefixed `SONATA_` (e.g. `SONATA_LOG_LEVEL=debug`)
3. Config file
4. Built-in defaults

Config file search path: `./sonata.yaml`, then `$XDG_CONFIG_HOME/sonata/config.yaml`
(falling back to `~/.config/sonata/config.yaml`).

```yaml
state_dir: ~/.local/state/sonata
socket: ~/.local/state/sonata/sonata.sock     # default: <state_dir>/sonata.sock
database: ~/.local/state/sonata/sonata.db     # default: <state_dir>/sonata.db

log_level: info
idle_timeout: 0s         # daemon self-terminates after this long idle; 0 disables
```

## Data model

The daemon owns these tables. Migrations live in `internal/store/migrations` and
run at startup.

- **messages** — `id` (UUIDv7, so lexicographic order is creation order),
  `queue`, `payload`, `headers` (including `hops`), `trace_id`,
  `origin_action` / `origin_action_version` / `origin_message_id` for causality,
  `created_at`
- **actions** — `(name, version)` primary key, canonical JSON `definition`,
  `enabled`, `created_at`. Versions are immutable; applying an edit inserts a
  row rather than updating one.
- **deliveries**, **join_waits** — the execution plane, landing with specs
  006–007

Timestamps are stored as RFC 3339 UTC; the CLI formats them for humans.

### Concurrency

Multiple CLI instances hitting the daemon at once is the normal case, so the
store layer is built for it:

- SQLite runs in **WAL** mode — concurrent readers never block the writer.
- Two connection handles: a **writer pinned to one connection**, and a pooled
  reader. Letting `database/sql` open several writing connections is what
  produces `SQLITE_BUSY` under concurrent load.
- Write transactions use **`BEGIN IMMEDIATE`**, taking the write lock up front.
  A default deferred transaction upgrades from a read lock on first write, and
  two of those upgrading simultaneously deadlock in a way `busy_timeout` cannot
  resolve.
- `busy_timeout` is set so readers wait through a checkpoint instead of erroring.
- Autostart is guarded by a lockfile: several CLI instances can discover a dead
  socket at the same moment, and exactly one must win the race to spawn a daemon
  while the rest wait and retry.

## Development

The Go toolchain version is pinned in `.tool-versions`. With
[asdf](https://asdf-vm.com) installed:

```sh
asdf install      # reads .tool-versions
```

```sh
make build        # ./bin/sonata
make test         # unit tests (-race -short)
make test-integ   # everything, including daemon-spawning tests
make cover        # coverage.html
make lint         # go vet + golangci-lint
make fmt tidy clean
```

Work against an isolated state directory — never point a dev daemon at your real
one:

```sh
export SONATA_DATABASE=/tmp/dev/sonata.db
export SONATA_SOCKET=/tmp/dev/sonata.sock
./bin/sonata daemon
```

Design and build order live in [`specs/`](specs/README.md). Each slice is a spec
that fixes its decisions before the code lands.

## Status

Early development. The action definition format and HTTP surface are not yet
stable.

| Slice | State |
| --- | --- |
| [001](specs/001-daemon-lifecycle.md) Daemon lifecycle — `up`/`down`/`status`/`daemon`, socket, idle timeout | implemented |
| [002](specs/002-startup-race-and-ensure-running.md) Autostart lockfile and startup race | implemented |
| [003](specs/003-queue-action-model.md) Queue/action model — design record | accepted |
| [004](specs/004-store-and-message-plane.md) Store + message plane — `send`, `message`, `queue` | implemented |
| [005](specs/005-action-definitions.md) Action definitions — parsing, CEL, validation, versioned `action apply` | implemented |
| [006](specs/006-scheduler-and-executor.md) Scheduler + executor — deliveries, retries, dead-letter, subprocess actor | planned |
| [007](specs/007-joins.md) Joins — correlation buffer, matching, TTL expiry | planned |
| [008](specs/008-sources-and-observability.md) Sources + observability — `schedule` actor, `trace`, `graph`, `prune` | planned |

**Registered actions are inert until slice 006.** You can send messages and
apply, version, and inspect actions today; nothing executes yet.

## License

MIT
