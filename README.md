# Sonata

Local workflow orchestration. A single Go binary is both a CLI and a daemon: the
daemon owns workflow state in an embedded SQLite database and executes runs,
while any number of CLI instances — yours, a script's, an LLM agent's — connect
to it concurrently to define, trigger, and inspect workflows.

Everything runs on your machine. No cluster, no broker, no network dependency.

## Why

Existing orchestrators (Airflow, Temporal, Prefect) assume a server, a message
broker, and a database you have to operate. Sonata targets durable, resumable,
dependency-aware task execution on one machine — build pipelines, data pulls,
local ML jobs, agent-driven work — without the operational surface.

## Architecture

```
┌──────────┐   ┌──────────┐   ┌──────────┐
│ sonata   │   │ sonata   │   │ sonata   │      CLI instances
│ run      │   │ status   │   │ submit   │      (concurrent, short-lived)
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    │  HTTP/JSON over a Unix domain socket
                    ▼
            ┌───────────────┐
            │ sonata daemon │  single process, sole DB writer
            │  ├ HTTP server│
            │  ├ scheduler  │
            │  └ executors  │
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
  -d '{"run_id":"r-4f21"}' http://x/v1/run.show
```

### Components

| Component | Responsibility |
| --- | --- |
| `cmd/sonata` | Entrypoint for both CLI and daemon modes |
| `internal/cli` | Cobra command tree, Viper config, output formatting |
| `internal/api` | HTTP handlers, client wrapper, shared request/response types |
| `internal/daemon` | Process lifecycle, socket setup, auto-start and locking |
| `internal/scheduler` | Dependency resolution, run queueing, retry and backoff |
| `internal/executor` | Task execution (subprocess), log capture, timeouts |
| `internal/store` | SQLite access layer, migrations, transaction boundaries |
| `internal/workflow` | Workflow definition parsing and validation |

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
$ sonata run etl
starting daemon...
run r-4f21 queued
```

To run it in the foreground instead — under systemd, launchd, or to watch logs:

```sh
sonata daemon              # foreground
sonata daemon --status
sonata daemon --stop
```

### Named workflows

For pipelines you re-run and keep in git. `workflows/etl.yaml`:

```yaml
name: etl
description: Nightly data refresh

tasks:
  - id: extract
    run: ./scripts/extract.sh
    timeout: 10m

  - id: transform
    run: python scripts/transform.py
    depends_on: [extract]
    retries: 2

  - id: load
    run: ./scripts/load.sh
    depends_on: [transform]
    env:
      TARGET: warehouse
```

```sh
sonata workflow apply workflows/etl.yaml
sonata run etl
sonata run etl --watch          # stream task state until the run finishes
```

### Ad-hoc runs

For one-off DAGs — typically an agent or script constructing work on the fly.
`submit` takes the same structure as JSON on stdin, runs it once, and registers
nothing:

```sh
echo '{
  "tasks": [
    {"id": "fetch", "run": "./fetch.sh"},
    {"id": "check", "run": "pytest", "depends_on": ["fetch"]}
  ]
}' | sonata submit --watch
```

YAML and JSON parse into the same internal type; the only difference is whether
the definition is persisted under a name.

### Inspecting and recovering

```sh
sonata workflow list
sonata run list --workflow etl --limit 20
sonata run show r-4f21
sonata logs r-4f21 --task transform --follow

sonata run retry r-4f21         # re-run failed tasks and their dependents
sonata run cancel r-4f21
```

Every command takes `--output json` for scripting.

## Command reference

| Command | Description |
| --- | --- |
| `sonata workflow apply <file>` | Create or update a named workflow |
| `sonata workflow list` | List registered workflows |
| `sonata workflow show <name>` | Show tasks and dependency graph |
| `sonata workflow delete <name>` | Remove a workflow (run history is retained) |
| `sonata run <name>` | Trigger a run of a named workflow |
| `sonata submit` | Run an ad-hoc DAG from JSON on stdin |
| `sonata run list` | List runs, newest first |
| `sonata run show <run-id>` | Per-task status, timings, exit codes |
| `sonata run retry <run-id>` | Resume a failed run |
| `sonata run cancel <run-id>` | Signal running tasks to stop |
| `sonata logs <run-id>` | Stream or dump captured task output |
| `sonata daemon` | Run the daemon in the foreground |
| `sonata daemon --stop\|--status` | Daemon lifecycle |

Global flags: `--config`, `--socket`, `--output json|table`, `--verbose`,
`--no-autostart`.

## Configuration

Viper resolves configuration in this precedence order:

1. Command-line flags
2. Environment variables prefixed `SONATA_` (e.g. `SONATA_LOG_LEVEL=debug`)
3. Config file
4. Built-in defaults

Config file search path: `./sonata.yaml`, then `$XDG_CONFIG_HOME/sonata/config.yaml`
(falling back to `~/.config/sonata/config.yaml`).

```yaml
socket: ~/.local/state/sonata/sonata.sock
database: ~/.local/state/sonata/sonata.db

log_level: info
autostart: true          # spawn a daemon when none is running

scheduler:
  max_concurrent_tasks: 4
  poll_interval: 1s

retention:
  runs: 30d              # prune run + log rows older than this
  adhoc_runs: 7d         # ad-hoc submissions expire sooner
```

## Data model

The daemon owns these tables. Migrations live in `internal/store/migrations` and
run at startup.

- **workflows** — name, version, definition, timestamps
- **tasks** — task nodes, command, retry policy
- **task_deps** — edges of the dependency DAG
- **runs** — one execution: state, trigger, start/end (ad-hoc runs carry an
  inline definition instead of a workflow reference)
- **task_runs** — per-task execution: state, attempt, exit code, timings
- **logs** — captured stdout/stderr chunks keyed to a task run

Run and task states: `pending` → `running` → `succeeded` | `failed` |
`cancelled` | `skipped`.

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

Work against an isolated state directory:

```sh
export SONATA_DATABASE=/tmp/dev/sonata.db
export SONATA_SOCKET=/tmp/dev/sonata.sock
./bin/sonata daemon
```

## Status

Early development. The workflow definition format and HTTP surface are not yet
stable.

## License

MIT
