# Extending hammerdown

Everything infrastructure-specific is a **backend**. The engine gives you
filters, planning, workers, locks, fail-fast, drain, stop-at, blackout and
reports for free.

```
             +--------------------- cmd/hammerdown ----------------------+
 operator -> | flags -> filter.Filter -> roll.Plan -> roll.Batch.Run      |
             |                 ^                        |                 |
             |                 |                        v                 |
             |        Backend.Inventory()      Backend.Processor()        |
             |                                 Backend.Locker()/Guards()  |
             +-----------------|------------------------|-----------------+
                               |                        |
                     your source of truth      your restart / failover
```

## The interfaces

```go
// backend/backend.go
type Factory interface {
	Name() string
	Describe() string
	RegisterFlags(fs *flag.FlagSet)
	Open(ctx context.Context, log roll.Logger) (Backend, error)
}

type Backend interface {
	Inventory() fleet.Inventory
	Processor() roll.Processor
	Locker() lock.Locker
	Guards() []roll.Guard
	Close(ctx context.Context) error
}

// roll/leader.go - implement this and use &roll.LeaderAware{Ops: ...}
type NodeOps interface {
	Restart(ctx context.Context, n *fleet.Node) error
	Failover(ctx context.Context, s *roll.Shard, from, to *fleet.Node) error
}
```

Wire it in `cmd/hammerdown/backends.go` and rebuild. That is the whole
dependency-injection story: no init() registration, no plugins.

## Worked example: Vitess (sketch)

This is the shape of a Vitess backend. It is a sketch to adapt, not
drop-in code: every shop stores "which build should this tablet run" a little
differently (a deploy agent, Consul KV, a Kubernetes CR ...).

```go
package vitess

type Factory struct {
	server  string // --vitess-server  vtctld gRPC address
	statusF string // --vitess-status  command printing tablet -> {running,target} JSON
	lockDir string
}

func (f *Factory) Name() string { return "vitess" }
func (f *Factory) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&f.server, "vitess-server", "localhost:15999", "vitess: vtctld gRPC address")
	fs.StringVar(&f.statusF, "vitess-status", "", "vitess: command printing deploy status JSON")
	fs.StringVar(&f.lockDir, "vitess-lock-dir", "/var/run/hammerdown", "vitess: lock dir")
}

func (f *Factory) Open(ctx context.Context, log roll.Logger) (backend.Backend, error) {
	client, err := vtctldclient.New("grpc", f.server)
	...
	return &be{client: client, ...}, nil
}

// Inventory: GetTablets for topology + your deploy-status source for builds.
func (b *be) Inventory() fleet.Inventory {
	return fleet.InventoryFunc(func(ctx context.Context) ([]*fleet.Node, error) {
		resp, err := b.client.GetTablets(ctx, &vtctldatapb.GetTabletsRequest{})
		...
		for _, t := range resp.Tablets {
			st := status[topoproto.TabletAliasString(t.Alias)]
			n := &fleet.Node{
				Name:    topoproto.TabletAliasString(t.Alias),
				Host:    t.Hostname,
				Keyspace: t.Keyspace,
				Shard:   t.Shard,
				Cell:    t.Alias.Cell,
				Role:    role(t.Type), // PRIMARY -> Primary, REPLICA -> Replica, RDONLY -> ReadOnly
				Running: st.Running,
				Target:  st.Target,
				State:   state(t.Type, st), // DRAINED -> Drained; Running != Target -> RestartWait
			}
			...
		}
	})
}

// Failover: PlannedReparentShard, then poll until `from` is no longer PRIMARY.
func (b *be) Failover(ctx context.Context, s *roll.Shard, from, to *fleet.Node) error {
	alias, _ := topoproto.ParseTabletAlias(to.Name)
	_, err := b.client.PlannedReparentShard(ctx, &vtctldatapb.PlannedReparentShardRequest{
		Keyspace:   s.Keyspace,
		Shard:      s.Name,
		NewPrimary: alias,
	})
	if err != nil {
		return err
	}
	if err := b.waitNotPrimary(ctx, from); err != nil {
		return err
	}
	from.Role, to.Role = fleet.Replica, fleet.Primary
	return nil
}

// Restart: whatever restarts vttablet in your shop (systemd over ssh, an
// agent API, deleting a pod), then poll until it's SERVING on the target build.
func (b *be) Restart(ctx context.Context, n *fleet.Node) error { ... }

// Guards: refuse shards with an in-flight tablet replacement or a running
// backup; check replication lag on the candidate before a failover.
```

Lessons from running this at ~11,000 tablets:

- **Use a planned reparent and wait for demotion.** Never restart the old
  primary until topology says it is a replica.
- **Only promote replicas already on the target build** (built into
  `LeaderAware`). Promoting an old-build or mixed-MySQL-version replica is
  how you break replication for a whole shard.
- **Take the same shard lock your other automation takes** (replacements,
  resharding, schema changes), not a private one.
- **gRPC keepalives.** Long rolls with many concurrent streams can trip
  server-side ping limits; set client keepalive to something polite (20s+).
- **Explicit workers beat ratios.** "64 shards at a time" is a number you
  can reason about at 3 a.m.; "2% of each keyspace" is not.

## Not a database? Write a Processor

`LeaderAware` is optional. Anything shard-shaped can be a `roll.Processor`:

```go
// Roll a config file to every node, one shard at a time, canary node first.
type ConfigPush struct{ Push func(ctx context.Context, n *fleet.Node) error }

func (p *ConfigPush) ProcessShard(ctx context.Context, job *roll.Job) error {
	for _, n := range job.Shard.Targets {
		if err := job.Stopped(); err != nil {
			return err // draining: fail-fast, stop-at or Ctrl-C
		}
		job.Logf("pushing config to %s", n)
		if job.DryRun {
			continue
		}
		if err := p.Push(ctx, n); err != nil {
			job.Fail(n, fmt.Errorf("%w: %s: %w", roll.ErrReplica, n, err))
			return nil // stop this shard, let fail-fast decide about the rest
		}
	}
	return nil
}
```

## Guards

```go
roll.GuardFunc(func(ctx context.Context, s *roll.Shard) error {
	if backupRunning(s.Keyspace, s.Name) {
		return errors.New("backup in progress")
	}
	return nil
})
```

A failing guard skips the shard (reported as a shard failure) and does not
count toward fail-fast.

## Asking Claude to do it

Open the repo in Claude Code. `CLAUDE.md` and the `add-backend` skill tell it
where the seams are and which invariants it must keep. A good prompt names
the inventory source, the restart command, the failover command and the lock:

> Add a `patroni` backend. Inventory: `patronictl -c /etc/patroni.yml list -f json`
> (keyspace = scope, shard = scope, role from Role). Restart: ssh + systemctl
> restart postgresql, healthy when `pg_isready`. Failover: `patronictl switchover`.
> Locks: lock.Dir under /var/run/hammerdown. Add tests with a fake patronictl.
