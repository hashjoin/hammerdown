---
name: add-backend
description: Add a new hammerdown backend (inventory + restart/failover + locks) for a specific system such as Vitess, Postgres/Patroni, MySQL, Redis, Kafka or a custom fleet. Use when asked to point hammerdown at real infrastructure.
---

# Add a hammerdown backend

Read `CLAUDE.md` first; its invariants are non-negotiable.

## 1. Interview (skip what the user already said)

- What system, and how do you list nodes today? (command, API, file)
- What does "needs a restart" look like in that data? (version mismatch,
  a flag, a deploy-status key)
- How is a node restarted, and how do you know it is healthy afterwards?
- How is a planned failover done, and how do you confirm demotion?
- What lock does the rest of your automation respect?
- Any pre-flight checks (replication lag, backups, maintenance windows)?

If the user has only scripts, propose the `shell` backend first; it may be
all they need.

## 2. Build

1. `backend/<name>/<name>.go`: `Factory` with `<name>-` prefixed flags,
   `Open` returning a `backend.Backend`.
2. Inventory: map source records to `fleet.Node`. Set `State` to
   `fleet.StateRestartWait` when Running != Target, `StateDrained` when out
   of service. If one shard can be read cheaply (one topo/API call), also
   implement `fleet.ShardInventory` (`ShardNodes`): hammerdown re-reads the
   shard under its lock and before every node operation, and refuses to act
   if roles changed. Reads must return fresh data, not a cached snapshot.
3. Processor: `&roll.LeaderAware{Ops: ops, Jitter: ...}` where `ops`
   implements `roll.NodeOps`. Restart and Failover must BLOCK until done and
   update node fields on success.
4. Locker: wrap the system's lock, ideally a lease that expires with its
   holder; otherwise `&lock.Dir{Path: ...}`. Never return an error that
   tells the operator to delete a lock. A lock that can outlive its holder
   implements `lock.Breaker` (see `lock.Dir`).
5. Register in `cmd/hammerdown/backends.go`.

## 3. Test

- Fake the external system (in-memory struct or a stub command).
- Run a real `roll.Batch` over 2 shards; assert operation order:
  replicas -> failover -> demoted primary.
- Assert dry run makes zero external calls.
- Assert locks are released after success and after a failure.
- Assert a stale plan is refused: change a role in the fake between `Plan`
  and `Run` and expect `roll.ErrStale` with no node operations (see
  `roll/stale_test.go`).
- `go vet ./... && go test -race ./...`

## 4. Hand-off

Show the user the exact commands for: report, dry run, a canary
(`--shards-per-keyspace 1`), then a real roll with conservative
`--workers` and `--max-primary-failures 1`.
