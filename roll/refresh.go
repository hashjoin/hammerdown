package roll

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hashjoin/hammerdown/fleet"
)

// ErrStale - the shard no longer looks the way it did when it was planned
// (or, mid-shard, the way this worker last saw it). Something else changed
// it: another hammerdown, another operator, an automatic failover. The shard
// is skipped untouched; rerunning replans it from fresh inventory.
var ErrStale = errors.New("shard changed since it was planned")

// Refresher re-reads one shard from the source of truth.
//
// The batch calls it after taking the shard lock and before the processor
// runs; LeaderAware calls it again (via Job.CheckRoles) right before every
// restart and failover. A plan is a snapshot; this is how the roll notices
// the snapshot is wrong before acting on it.
type Refresher interface {
	RefreshShard(ctx context.Context, keyspace, shard string) ([]*fleet.Node, error)
}

// RefresherFunc adapts a function to Refresher.
type RefresherFunc func(ctx context.Context, keyspace, shard string) ([]*fleet.Node, error)

// RefreshShard implements Refresher.
func (f RefresherFunc) RefreshShard(ctx context.Context, keyspace, shard string) ([]*fleet.Node, error) {
	return f(ctx, keyspace, shard)
}

// InventoryRefresher re-reads shards from inv: through ShardNodes when inv
// implements fleet.ShardInventory, otherwise by listing the whole fleet and
// keeping the shard's nodes.
func InventoryRefresher(inv fleet.Inventory) Refresher {
	if si, ok := inv.(fleet.ShardInventory); ok {
		return RefresherFunc(si.ShardNodes)
	}

	return RefresherFunc(func(ctx context.Context, keyspace, shard string) ([]*fleet.Node, error) {
		all, err := inv.Nodes(ctx)
		if err != nil {
			return nil, err
		}
		var out []*fleet.Node
		for _, n := range all {
			if n.Keyspace == keyspace && n.Shard == shard {
				out = append(out, n)
			}
		}

		return out, nil
	})
}

// view is the part of a node that decides what a roll would do to it.
type view struct {
	Role    fleet.Role
	State   fleet.State
	Running string
}

func snapshot(nodes []*fleet.Node) map[string]view {
	m := make(map[string]view, len(nodes))
	for _, n := range nodes {
		m[n.Name] = view{Role: n.Role, State: n.State, Running: n.Running}
	}

	return m
}

// diff reports how fresh disagrees with base (up to three differences), or
// nil. rolesOnly compares roles (and primaries) only: mid-shard, this
// worker's own restarts legitimately change State and Running.
func diff(base map[string]view, fresh []*fleet.Node, rolesOnly bool) error {
	byName := make(map[string]*fleet.Node, len(fresh))
	for _, n := range fresh {
		byName[n.Name] = n
	}

	names := make([]string, 0, len(base))
	for name := range base {
		names = append(names, name)
	}
	sort.Strings(names)

	var changes []string
	for _, name := range names {
		was := base[name]
		n, ok := byName[name]
		switch {
		case !ok:
			changes = append(changes, name+" is no longer in the shard")
		case n.Role != was.Role:
			changes = append(changes, fmt.Sprintf("%s was %s, now %s", n, was.Role, n.Role))
		case !rolesOnly && (n.State != was.State || n.Running != was.Running):
			changes = append(changes, fmt.Sprintf("%s was %s on %s, now %s on %s", n, was.State, was.Running, n.State, n.Running))
		}
	}
	for _, n := range fresh {
		if _, ok := base[n.Name]; !ok && n.IsPrimary() {
			changes = append(changes, n.String()+" is a new primary")
		}
	}

	switch {
	case len(changes) == 0:
		return nil
	case len(changes) > 3:
		changes = append(changes[:3], fmt.Sprintf("and %d more", len(changes)-3))
	}

	return errors.New(strings.Join(changes, "; "))
}

// refresh re-reads job's shard and compares it with the plan.
func (b *Batch) refresh(ctx context.Context, s *Shard) error {
	if b.Refresher == nil {
		return nil
	}

	fresh, err := b.Refresher.RefreshShard(ctx, s.Keyspace, s.Name)
	if err != nil {
		return fmt.Errorf("re-reading %s before touching it: %w", s.Key(), err)
	}
	if err := diff(s.planned, fresh, false); err != nil {
		return fmt.Errorf("%w: %w (skipped untouched; rerun to replan)", ErrStale, err)
	}

	return nil
}

// CheckRoles re-reads the shard and returns an error wrapping ErrStale if
// any node's role differs from what this worker believes now (its own
// failovers included). Call it right before a node operation; return the
// error from ProcessShard. A no-op when the batch has no Refresher.
//
// This is the "refresh metadata and check this isn't a primary" step: the
// plan said replica, but a plan is only a snapshot.
func (j *Job) CheckRoles(ctx context.Context) error {
	r := j.batch.Refresher
	if r == nil {
		return nil
	}

	fresh, err := r.RefreshShard(ctx, j.Shard.Keyspace, j.Shard.Name)
	if err != nil {
		return fmt.Errorf("re-reading %s: %w", j.Shard.Key(), err)
	}
	if err := diff(snapshot(j.Shard.Members), fresh, true); err != nil {
		return fmt.Errorf("%w: %w (stopped this shard; rerun to replan)", ErrStale, err)
	}

	return nil
}
