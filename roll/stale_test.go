package roll

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
)

// topo is a fake source of truth. Like a real topology server, every read
// returns fresh copies: two hammerdown processes each hold their own
// snapshot and neither sees the other's changes unless it re-reads.
type topo struct {
	mu    sync.Mutex
	truth map[string]*fleet.Node
	order []string // every node operation, by any instance
	// afterRestart runs (under mu) after each restart, to simulate
	// something else changing the shard mid-roll.
	afterRestart func(t *topo, n *fleet.Node)
}

func newTopo(nodes []*fleet.Node) *topo {
	t := &topo{truth: map[string]*fleet.Node{}}
	for _, n := range nodes {
		t.truth[n.Name] = n
	}

	return t
}

func (t *topo) read(keep func(*fleet.Node) bool) []*fleet.Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []*fleet.Node
	for _, n := range t.truth {
		if keep(n) {
			c := *n
			out = append(out, &c)
		}
	}

	return out
}

func (t *topo) Nodes(context.Context) ([]*fleet.Node, error) {
	return t.read(func(*fleet.Node) bool { return true }), nil
}

func (t *topo) ShardNodes(_ context.Context, keyspace, shard string) ([]*fleet.Node, error) {
	return t.read(func(n *fleet.Node) bool { return n.Keyspace == keyspace && n.Shard == shard }), nil
}

// ops applies node operations to the truth, recording the role the node
// ACTUALLY had when it was restarted - not the role the caller believed.
type topoOps struct{ t *topo }

func (o topoOps) Restart(_ context.Context, n *fleet.Node) error {
	o.t.mu.Lock()
	defer o.t.mu.Unlock()

	actual := o.t.truth[n.Name]
	o.t.order = append(o.t.order, "restart "+n.Name+" (really "+string(actual.Role)+")")
	actual.Running, actual.State = actual.Target, fleet.StateOK
	n.Running, n.State = n.Target, fleet.StateOK
	if o.t.afterRestart != nil {
		o.t.afterRestart(o.t, n)
	}

	return nil
}

func (o topoOps) Failover(_ context.Context, _ *Shard, from, to *fleet.Node) error {
	o.t.mu.Lock()
	defer o.t.mu.Unlock()

	o.t.order = append(o.t.order, "failover "+from.Name+"->"+to.Name)
	o.t.truth[from.Name].Role, o.t.truth[to.Name].Role = fleet.Replica, fleet.Primary
	from.Role, to.Role = fleet.Replica, fleet.Primary

	return nil
}

func (t *topo) primaryRestarts() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []string
	for _, op := range t.order {
		if strings.HasSuffix(op, "(really primary)") {
			out = append(out, op)
		}
	}

	return out
}

// The two-instance shape: two copies of the roll, each with its own
// snapshot of the fleet. B plans, then A rolls the same shards to completion
// (failing every primary over), then B runs its now-stale plan. Locks don't
// save you here: A has finished and released them by the time B takes them.
func twoInstances(t *testing.T, refresh bool) (*topo, *Result, error) {
	t.Helper()
	ctx := context.Background()
	tp := newTopo(fleetOf(1, 3, 2))
	locks := lock.NewMemory()
	f := &filter.Filter{}

	newBatch := func() *Batch {
		b := &Batch{Processor: &LeaderAware{Ops: topoOps{tp}}, Locker: locks, Log: quiet, Workers: 2}
		if refresh {
			b.Refresher = InventoryRefresher(tp)
		}

		return b
	}

	snapB, _ := tp.Nodes(ctx)
	planB := Plan(snapB, f) // B's view: n0 is every shard's primary

	snapA, _ := tp.Nodes(ctx)
	if _, err := newBatch().Run(ctx, Plan(snapA, f)); err != nil {
		t.Fatalf("instance A: %v", err)
	}
	if got := tp.primaryRestarts(); len(got) != 0 {
		t.Fatalf("instance A alone restarted primaries: %v", got)
	}

	res, err := newBatch().Run(ctx, planB)

	return tp, res, err
}

// Control: without a re-read, B restarts the primaries A just promoted.
// This is the bug; the test exists to prove the scenario is real.
func TestStalePlanWithoutRefresherRestartsPrimaries(t *testing.T) {
	tp, _, _ := twoInstances(t, false)
	if len(tp.primaryRestarts()) == 0 {
		t.Fatal("expected the stale instance to restart live primaries; the scenario no longer reproduces")
	}
}

func TestStalePlanIsSkippedUnderShardLock(t *testing.T) {
	tp, res, err := twoInstances(t, true)

	if got := tp.primaryRestarts(); len(got) != 0 {
		t.Fatalf("stale instance restarted live primaries: %v", got)
	}
	if !errors.Is(err, ErrFailures) {
		t.Fatalf("expected ErrFailures, got %v", err)
	}
	if len(res.ShardFailures) != 3 {
		t.Fatalf("want 3 stale shards, got %+v", res.ShardFailures)
	}
	for _, f := range res.ShardFailures {
		if !errors.Is(f.Err, ErrStale) {
			t.Errorf("%s: want ErrStale, got %v", f.Shard, f.Err)
		}
	}
	if res.PrimaryFailures+res.ReplicaFailures != 0 {
		t.Errorf("stale shards must not count as node failures: %+v", res)
	}
}

// The per-node check: a replica that becomes primary mid-shard (an
// automatic failover, another tool) is not restarted.
func TestReplicaBecamePrimaryMidShard(t *testing.T) {
	tp := newTopo(fleetOf(1, 1, 2)) // n0 primary, n1 n2 replicas
	tp.afterRestart = func(t *topo, n *fleet.Node) {
		if n.Name == "c0-s0-n1" { // someone else promotes n2 behind our back
			t.truth["c0-s0-n0"].Role, t.truth["c0-s0-n2"].Role = fleet.Replica, fleet.Primary
		}
	}

	snap, _ := tp.Nodes(context.Background())
	b := &Batch{Processor: &LeaderAware{Ops: topoOps{tp}}, Refresher: InventoryRefresher(tp), Log: quiet, Workers: 1}
	res, err := b.Run(context.Background(), Plan(snap, &filter.Filter{}))

	if got := tp.primaryRestarts(); len(got) != 0 {
		t.Fatalf("restarted a node that became primary: %v", got)
	}
	if want := []string{"restart c0-s0-n1 (really replica)"}; strings.Join(tp.order, ",") != strings.Join(want, ",") {
		t.Errorf("ops %v, want %v", tp.order, want)
	}
	if !errors.Is(err, ErrFailures) || len(res.ShardFailures) != 1 || !errors.Is(res.ShardFailures[0].Err, ErrStale) {
		t.Fatalf("want one ErrStale shard failure, got err=%v res=%+v", err, res.ShardFailures)
	}
	if msg := res.ShardFailures[0].Err.Error(); !strings.Contains(msg, "was replica, now primary") {
		t.Errorf("message should say what changed: %q", msg)
	}
}

// A fresh plan against an unchanged fleet rolls normally with re-reads on.
func TestRefresherIsQuietWhenNothingChanged(t *testing.T) {
	tp := newTopo(fleetOf(2, 4, 2))
	snap, _ := tp.Nodes(context.Background())
	b := &Batch{Processor: &LeaderAware{Ops: topoOps{tp}}, Refresher: InventoryRefresher(tp), Locker: lock.NewMemory(), Log: quiet, Workers: 4}
	if _, err := b.Run(context.Background(), Plan(snap, &filter.Filter{})); err != nil {
		t.Fatal(err)
	}
	for _, n := range tp.truth {
		if n.Running != n.Target {
			t.Errorf("%s not rolled", n)
		}
	}
}

// Without ShardNodes the refresher falls back to a full listing.
func TestInventoryRefresherFallback(t *testing.T) {
	nodes := fleetOf(1, 2, 1)
	r := InventoryRefresher(fleet.InventoryFunc(func(context.Context) ([]*fleet.Node, error) { return nodes, nil }))
	got, err := r.RefreshShard(context.Background(), "c0", "s001")
	if err != nil || len(got) != 2 || got[0].Shard != "s001" {
		t.Fatalf("got %v %v", got, err)
	}
}
