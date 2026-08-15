# Sonata

Local workflow orchestration. A single Go binary provides both a CLI and a
long-running daemon: the daemon owns workflow state in an embedded SQLite
database and executes runs, while any number of CLI instances connect to it to
define, trigger, and inspect workflows.

Everything runs on your machine. No cluster, no external services, no network
dependency.

## Why

Existing orchestrators (Airflow, Temporal, Prefect) assume a server, a message
broker, and a database you have to operate. Sonata targets the case where you
want durable, resumable, dependency-aware task execution on one machine — build
pipelines, data pulls, local ML jobs, scheduled maintenance — without the
operational surface.

## Architecture

```
┌──────────┐   ┌──────────┐   ┌──────────┐
│ sonata   │   │ sonata   │   │ sonata   │      CLI instances
│ (run)    │   │ (status) │   │ (logs)   │      (short-lived)
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    │  Unix domain socket (gRPC)
                    ▼
            ┌───────────────┐
            │ sonatad       │  daemon (single writer)
            │  ├ API server │
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
CLI instances never open the DB file directly. This keeps concurrency handling in
one place and makes the client a thin RPC wrapper.

### Components

| Component | Responsibility |
| --- | --- |
| `cmd/sonata` | Cobra command tree, Viper config resolution, output formatting |
| `cmd/sonatad` | Daemon entrypoint, lifecycle, signal handling |
| `internal/api` | gRPC service definitions and handlers over the Unix socket |
| `internal/scheduler` | Dependency resolution, run queueing, retry and backoff |
| `internal/executor` | Task execution (subprocess), log capture, timeouts |
| `internal/store` | SQLite access layer, migrations, transaction boundaries |
| `internal/workflow` | Workflow definition parsing and validation |

## Installation

Requires Go 1.22+.

```sh
go install github.com/matthewcondie/sonata/cmd/sonata@latest
go install github.com/matthewcondie/sonata/cmd/sonatad@latest
```

Or from a clone:

```sh
make build      # binaries land in ./bin
make install    # copies to $GOBIN
```

## Quick start

Start the daemon (it also starts on demand when a CLI command needs it):

```sh
sonatad start
```

Define a workflow in `workflows/etl.yaml`:

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

Register and run it:

```sh
sonata workflow apply workflows/etl.yaml
sonata run etl
sonata run etl --watch          # stream task state until the run finishes
```

Inspect:

```sh
sonata workflow list
sonata run list --workflow etl --limit 20
sonata run show <run-id>
sonata logs <run-id> --task transform --follow
```

Recover:

```sh
sonata run retry <run-id>           # re-run only failed tasks and their dependents
sonata run cancel <run-id>
```

## Command reference

| Command | Description |
| --- | --- |
| `sonata workflow apply <file>` | Create or update a workflow definition |
| `sonata workflow list` | List registered workflows |
| `sonata workflow show <name>` | Show a workflow's tasks and dependency graph |
| `sonata workflow delete <name>` | Remove a workflow (run history is retained) |
| `sonata run <name>` | Trigger a run |
| `sonata run list` | List runs, newest first |
| `sonata run show <run-id>` | Per-task status, timings, exit codes |
| `sonata run retry <run-id>` | Resume a failed run |
| `sonata run cancel <run-id>` | Signal running tasks to stop |
| `sonata logs <run-id>` | Stream or dump captured task output |
| `sonatad start\|stop\|status` | Daemon lifecycle |

Global flags: `--config`, `--socket`, `--output json|table`, `--verbose`.

## Configuration

Viper resolves configuration in this precedence order:

1. Command-line flags
2. Environment variables prefixed `SONATA_` (e.g. `SONATA_LOG_LEVEL=debug`)
3. Config file
4. Built-in defaults

Config file search path: `./sonata.yaml`, then `$XDG_CONFIG_HOME/sonata/config.yaml`
(falling back to `~/.config/sonata/config.yaml`).

```yaml
socket: ~/.local/state/sonata/sonatad.sock
database: ~/.local/state/sonata/sonata.db

log_level: info

scheduler:
  max_concurrent_tasks: 4
  poll_interval: 1s

retention:
  runs: 30d          # prune run + log rows older than this
```

## Data model

The daemon owns these tables. Migrations live in `internal/store/migrations` and
run automatically at daemon startup.

- **workflows** — name, version, definition, timestamps
- **tasks** — workflow's task nodes, command, retry policy
- **task_deps** — edges of the dependency DAG
- **runs** — one execution of a workflow: state, trigger, start/end
- **task_runs** — per-task execution: state, attempt, exit code, timings
- **logs** — captured stdout/stderr chunks keyed to a task run

Run and task states: `pending` → `running` → `succeeded` | `failed` |
`cancelled` | `skipped`.

SQLite runs in WAL mode with `busy_timeout` set, so read-only queries never block
the writer.

## Development

```sh
make build        # compile both binaries
make test         # unit tests
make test-integ   # integration tests (spawns a real daemon in a temp dir)
make lint         # go vet + golangci-lint
make proto        # regenerate gRPC code from proto/
```

Run against an isolated state directory while developing:

```sh
SONATA_DATABASE=/tmp/dev/sonata.db SONATA_SOCKET=/tmp/dev/sonatad.sock ./bin/sonatad start
```

## Status

Early development. The workflow definition format and RPC surface are not yet
stable.

## License

MIT
