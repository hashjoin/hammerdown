# CLAUDE.md

Guidance for Claude (and humans) working in this repo.

## What this is

`hammerdown` is a CLI control plane for rolling restarts/upgrades across
sharded fleets. The engine (filter, plan, schedule, lock, fail-fast, report)
is generic; everything infrastructure-specific lives in a **backend**.
Most changes people ask for are "add a backend for X" or "add a filter/guard".

## Commands

```sh
make fmt lint test        # gofumpt+goimports, golangci-lint (.golangci.yml), race tests - must stay green
go run ./cmd/hammerdown sim init --force
go run ./cmd/hammerdown sim stage --to v20.0.1
go run ./cmd/hammerdown report
go run ./cmd/hammerdown roll --workers 64 --dry-run=false --yes --quiet
make demo        # the above, end to end
```

## Map

| Path | Role |
|---|---|
| `fleet/fleet.go` | `Node` (keyspace, shard, role, state, running/target build), `Inventory` |
| `filter/filter.go` | `Spec` parser (`users/ALL\|teams/-80,80-`, globs), `Filter.Match` / `InScope` |
| `roll/plan.go` | `Plan`: nodes + filter -> sorted `[]*Shard`, replicas before primary |
| `roll/batch.go` | `Batch.Run`: worker slots, per-keyspace cap, locks, guards, fail-fast, drain, stop-at, blackout |
| `roll/leader.go` | `LeaderAware` processor + `NodeOps` (Restart, Failover) |
| `lock/lock.go` | `Locker`/`Lock` + `Memory` and `Dir` |
| `report/report.go` | ASCII + JSON reports |
| `backend/backend.go` | `Backend` + `Factory` interfaces |
| `backend/sim` | simulated 11,000-node fleet; reference backend |
| `backend/shell` | script-driven backend; reference for templates/env |
| `cmd/hammerdown/backends.go` | **the only place backends are wired in** |

## Invariants - never break these

1. **Dry run touches nothing.** With `job.DryRun` a processor only logs. No
   NodeOps calls, no locks (the batch already skips them).
2. **Check `job.Stopped()` before every node operation.** That is what makes
   fail-fast exact and Ctrl-C graceful. Pass `ctx` into node operations; it
   is only cancelled on a hard abort.
3. **Classify failures.** `job.Fail(node, fmt.Errorf("%w: ...", roll.ErrPrimary, ...))`
   for anything on the failover/promotion path; everything else is replica
   (`roll.ErrReplica`). Shard-level problems are returned from `ProcessShard`.
4. **NodeOps block.** `Restart` returns only once the node is healthy on
   `n.Target` and then sets `n.Running = n.Target; n.State = fleet.StateOK`.
   `Failover` returns only once `from` is demoted and then swaps the roles.
5. **Never fail over to a node that isn't on the primary's target build.**
   `roll.candidates` enforces this; don't bypass it.
6. **Standard library only.** No new modules without a very good reason
   (backends for specific systems may need a client library: keep those in
   their own package so the core stays dependency-free).
7. **Output stays greppable.** Logs go to stderr via `job.Logf` (prefix
   `( dry-run=.. ) ( worker=NN ) ...`); reports go to stdout as fixed-width ASCII.

## Recipe: add a backend

1. Create `backend/<name>/<name>.go` with a `Factory` implementing
   `backend.Factory` (`Name`, `Describe`, `RegisterFlags`, `Open`).
   Prefix every flag with `<name>-`.
2. `Open` returns a type implementing `backend.Backend`:
   - `Inventory()` -> `fleet.InventoryFunc` that maps your source of truth
     (topo server, CMDB, cloud API, Consul) to `[]*fleet.Node`. Map "needs a
     restart" to `State: fleet.StateRestartWait`.
   - `Processor()` -> `&roll.LeaderAware{Ops: yourOps, Jitter: ...}` for
     primary/replica systems, or your own `roll.Processor` otherwise.
   - `Locker()` -> a lock your other automation honours (Consul, etcd, topo).
     `&lock.Dir{...}` is acceptable for single-operator use.
   - `Guards()` -> pre-flight checks (replication lag, backups, replacements).
3. Add `<name>.NewFactory()` to `cmd/hammerdown/backends.go`.
4. Tests: copy the shape of `backend/shell/shell_test.go` - fake the external
   system, run a real `roll.Batch` over 1-2 shards, assert the exact order of
   operations and that locks are released.
5. Document the flags in the package comment and add a row to README.

See `docs/extending.md` for a worked Vitess example and a non-database example.

## Recipe: add a filter

Add the field to `filter.Filter`, the predicate to `Match` (or `InScope` if
it is a "where" filter reports should honour too), the flag in
`cmd/hammerdown/common.go`, and a case in `filter/filter_test.go`. Return a
human reason string for rejections; `--debug` prints them.

## Style

- Errors wrap with `%w` and say what was being done to which node.
- Keep functions small and the scheduler boring. Cleverness goes in backends.
- Tests use the race detector and fake time/latency; no sleeps over ~50ms.
