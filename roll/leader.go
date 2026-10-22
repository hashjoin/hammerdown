package roll

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/hashjoin/hammerdown/fleet"
)

// NodeOps is the smallest thing you implement to roll a primary/replica
// system with LeaderAware. Two verbs, both blocking:
//
//   - Restart: restart n onto n.Target and return only once it is healthy
//     and serving (or ctx is done). On success set n.Running = n.Target and
//     n.State = fleet.StateOK.
//   - Failover: planned, graceful promotion of `to` and demotion of `from`.
//     Return only once `from` is no longer primary. On success set
//     from.Role = fleet.Replica and to.Role = fleet.Primary.
//
// For Vitess that is "restart vttablet and wait for SERVING" and
// "PlannedReparentShard"; for Postgres+Patroni "systemctl restart" and
// "patronictl switchover"; for Redis "restart" and "CLUSTER FAILOVER".
type NodeOps interface {
	Restart(ctx context.Context, n *fleet.Node) error
	Failover(ctx context.Context, s *Shard, from, to *fleet.Node) error
}

// ChooseFunc picks the promotion candidate from eligible replicas.
type ChooseFunc func(primary *fleet.Node, eligible []*fleet.Node) *fleet.Node

// LeaderAware is the bundled Processor for primary/replica shards:
//
//  1. restart the replica targets one by one (new build lands on them first);
//  2. if any replica failed, leave the primary alone;
//  3. pick a candidate already running the primary's target build;
//  4. planned failover primary -> candidate;
//  5. restart the old primary, now a replica.
type LeaderAware struct {
	Ops NodeOps
	// Jitter - sleep Jitter + rand(Jitter)/2 before each restart to splay
	// load on the things restarts hammer (topo server, service discovery).
	Jitter time.Duration
	// Choose overrides candidate selection (default: prefer a different
	// cell than the old primary, then by name).
	Choose ChooseFunc
}

// ProcessShard implements Processor.
func (p *LeaderAware) ProcessShard(ctx context.Context, job *Job) error {
	var (
		primaries []*fleet.Node
		restarted []*fleet.Node
		failed    int
	)

	for _, n := range job.Shard.Targets {
		if n.IsPrimary() {
			primaries = append(primaries, n)

			continue
		}

		if err := p.restart(ctx, job, n, "replica"); err != nil {
			if stopped(ctx, job) {
				return fmt.Errorf("stopped at %s: %w", n, err)
			}
			failed++
			job.Fail(n, fmt.Errorf("%w: restart %s: %w", ErrReplica, n, err))

			continue
		}
		restarted = append(restarted, n)
	}

	if len(primaries) == 0 {
		return nil
	}
	if failed > 0 {
		return fmt.Errorf("%d replica restart failure(s): leaving primary %s alone", failed, primaries[0])
	}
	if len(primaries) > 1 {
		return fmt.Errorf("shard has %d primaries: refusing to touch it", len(primaries))
	}

	primary := primaries[0]
	if err := job.Stopped(); err != nil {
		return fmt.Errorf("stopped before failover of %s: %w", primary, err)
	}

	eligible := candidates(primary, restarted, job.Shard.Members, job.DryRun)
	if len(eligible) == 0 {
		err := fmt.Errorf("%w: no failover candidate for %s running %s", ErrPrimary, primary, primary.Target)
		if job.DryRun {
			job.Logf("WARNING: would fail: %v", err)

			return nil
		}
		job.Fail(primary, err)

		return nil
	}

	choose := p.Choose
	if choose == nil {
		choose = DefaultChoose
	}
	to := choose(primary, eligible)

	job.Logf("planned failover %s -> %s (%d eligible)", primary, to.Host, len(eligible))
	if !job.DryRun {
		if err := p.Ops.Failover(ctx, job.Shard, primary, to); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("aborted during failover of %s: %w", primary, err)
			}
			job.Fail(primary, fmt.Errorf("%w: failover %s -> %s: %w", ErrPrimary, primary, to.Host, err))

			return nil
		}
	}

	if err := p.restart(ctx, job, primary, "demoted primary"); err != nil {
		if stopped(ctx, job) {
			return fmt.Errorf("stopped at demoted primary %s: %w", primary, err)
		}
		job.Fail(primary, fmt.Errorf("%w: restart demoted primary %s: %w", ErrReplica, primary, err))
	}

	return nil
}

// restart: drain check -> jitter -> drain check -> Ops.Restart.
func (p *LeaderAware) restart(ctx context.Context, job *Job, n *fleet.Node, what string) error {
	if err := job.Stopped(); err != nil {
		return err
	}

	job.Logf("restarting %s %s (%s -> %s)", what, n, n.Running, n.Target)
	if job.DryRun {
		return nil
	}

	if err := sleepJitter(ctx, p.Jitter); err != nil {
		return err
	}
	if err := job.Stopped(); err != nil {
		return err
	}

	return p.Ops.Restart(ctx, n)
}

// stopped: the error came from draining or a hard abort, not from the node.
func stopped(ctx context.Context, job *Job) bool {
	return ctx.Err() != nil || job.Stopped() != nil
}

// candidates: replicas (not rdonly, not drained, not the primary) that
// run the primary's target build - either just restarted or already there.
func candidates(primary *fleet.Node, restarted, members []*fleet.Node, dryRun bool) []*fleet.Node {
	seen := map[string]bool{}
	var out []*fleet.Node

	add := func(n *fleet.Node, assumeUpgraded bool) {
		if seen[n.Name] || n.Name == primary.Name || n.Role != fleet.Replica || n.State == fleet.StateDrained {
			return
		}
		if !assumeUpgraded && n.Running != primary.Target {
			return
		}
		seen[n.Name] = true
		out = append(out, n)
	}

	// in a dry run nothing was actually restarted; pretend it was
	for _, n := range restarted {
		add(n, dryRun)
	}
	for _, n := range members {
		add(n, false)
	}

	return out
}

// DefaultChoose prefers a candidate in a different cell than the old
// primary (spreads primaries across failure domains after a full roll),
// then sorts by name for determinism.
func DefaultChoose(primary *fleet.Node, eligible []*fleet.Node) *fleet.Node {
	sorted := append([]*fleet.Node(nil), eligible...)
	sort.SliceStable(sorted, func(i, j int) bool {
		di, dj := sorted[i].Cell != primary.Cell, sorted[j].Cell != primary.Cell
		if di != dj {
			return di
		}

		return sorted[i].Name < sorted[j].Name
	})

	return sorted[0]
}

func sleepJitter(ctx context.Context, base time.Duration) error {
	if base <= 0 {
		return nil
	}

	d := base + time.Duration(rand.Int63n(int64(base)))/2 // #nosec G404 - jitter, not crypto

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}
