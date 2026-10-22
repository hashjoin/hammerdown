package roll

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
)

var quiet = log.New(io.Discard, "", 0)

// fleetOf builds keyspaces x shards x (1 primary + replicas), all RestartWait.
func fleetOf(keyspaces, shards, replicas int) []*fleet.Node {
	var nodes []*fleet.Node
	for c := 0; c < keyspaces; c++ {
		for s := 0; s < shards; s++ {
			for r := 0; r <= replicas; r++ {
				role := fleet.Replica
				if r == 0 {
					role = fleet.Primary
				}
				nodes = append(nodes, &fleet.Node{
					Name:     fmt.Sprintf("c%d-s%d-n%d", c, s, r),
					Host:     fmt.Sprintf("h-c%d-s%d-n%d", c, s, r),
					Keyspace: fmt.Sprintf("c%d", c),
					Shard:    fmt.Sprintf("s%03d", s),
					Cell:     fmt.Sprintf("az%d", r%3),
					Role:     role,
					State:    fleet.StateRestartWait,
					Running:  "v1",
					Target:   "v2",
				})
			}
		}
	}

	return nodes
}

type fakeOps struct {
	delay      time.Duration
	failHost   map[string]bool
	failOver   map[string]bool
	mu         sync.Mutex
	order      []string
	restarts   atomic.Int64
	failovers  atomic.Int64
	inflight   atomic.Int64
	maxInfight atomic.Int64
}

func (o *fakeOps) track() func() {
	v := o.inflight.Add(1)
	for {
		m := o.maxInfight.Load()
		if v <= m || o.maxInfight.CompareAndSwap(m, v) {
			break
		}
	}

	return func() { o.inflight.Add(-1) }
}

func (o *fakeOps) Restart(ctx context.Context, n *fleet.Node) error {
	o.mu.Lock()
	o.order = append(o.order, "restart "+n.Name)
	o.mu.Unlock()
	o.restarts.Add(1)
	time.Sleep(o.delay)
	if o.failHost[n.Host] {
		return errors.New("boom")
	}
	n.Running, n.State = n.Target, fleet.StateOK

	return nil
}

func (o *fakeOps) Failover(ctx context.Context, s *Shard, from, to *fleet.Node) error {
	o.mu.Lock()
	o.order = append(o.order, "failover "+from.Name+"->"+to.Name)
	o.mu.Unlock()
	o.failovers.Add(1)
	if o.failOver[s.Key()] {
		return errors.New("reparent timed out")
	}
	from.Role, to.Role = fleet.Replica, fleet.Primary

	return nil
}

// trackingProcessor wraps LeaderAware to measure shard concurrency.
type trackingProcessor struct {
	ops   *fakeOps
	inner Processor
	perCl sync.Map // keyspace -> *atomic.Int64 (current)
	maxCl atomic.Int64
}

func (t *trackingProcessor) ProcessShard(ctx context.Context, job *Job) error {
	done := t.ops.track()
	defer done()

	v, _ := t.perCl.LoadOrStore(job.Shard.Keyspace, &atomic.Int64{})
	c := v.(*atomic.Int64)
	cur := c.Add(1)
	defer c.Add(-1)
	for {
		m := t.maxCl.Load()
		if cur <= m || t.maxCl.CompareAndSwap(m, cur) {
			break
		}
	}

	return t.inner.ProcessShard(ctx, job)
}

func TestRollConvergesAndRespectsWorkers(t *testing.T) {
	nodes := fleetOf(4, 25, 2)
	shards := Plan(nodes, &filter.Filter{})
	if len(shards) != 100 {
		t.Fatalf("planned %d shards", len(shards))
	}

	ops := &fakeOps{delay: time.Millisecond}
	tp := &trackingProcessor{ops: ops, inner: &LeaderAware{Ops: ops}}
	lk := lock.NewMemory()

	b := &Batch{Processor: tp, Locker: lk, Log: quiet, Workers: 7, KeyspaceWorkers: 3}
	res, err := b.Run(context.Background(), shards)
	if err != nil {
		t.Fatal(err)
	}

	if got := ops.maxInfight.Load(); got > 7 {
		t.Errorf("max in-flight shards %d > workers 7", got)
	}
	if got := tp.maxCl.Load(); got > 3 {
		t.Errorf("max in-flight per keyspace %d > 3", got)
	}
	if res.ShardsDone != 100 || res.Failed() {
		t.Errorf("result: %+v", res)
	}
	for _, n := range nodes {
		if n.Running != "v2" {
			t.Fatalf("%s not upgraded", n.Name)
		}
	}
	if ops.failovers.Load() != 100 {
		t.Errorf("failovers = %d", ops.failovers.Load())
	}
	if lk.Held("process/roll") {
		t.Error("process lock leaked")
	}
}

func TestReplicasBeforePrimary(t *testing.T) {
	nodes := fleetOf(1, 1, 2)
	ops := &fakeOps{}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 1}
	if _, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{})); err != nil {
		t.Fatal(err)
	}

	want := []string{"restart c0-s0-n1", "restart c0-s0-n2", "failover c0-s0-n0->c0-s0-n1", "restart c0-s0-n0"}
	if fmt.Sprint(ops.order) != fmt.Sprint(want) {
		t.Errorf("order %v\nwant  %v", ops.order, want)
	}
}

func TestReplicaFailureSparesPrimary(t *testing.T) {
	nodes := fleetOf(1, 1, 2)
	ops := &fakeOps{failHost: map[string]bool{"h-c0-s0-n1": true}}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 1}

	res, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{}))
	if !errors.Is(err, ErrFailures) {
		t.Fatalf("expected ErrFailures, got %v", err)
	}
	if ops.failovers.Load() != 0 {
		t.Error("primary must not be failed over after a replica failure")
	}
	if len(res.ShardFailures) != 1 || res.ReplicaFailures != 1 {
		t.Errorf("res: %+v", res)
	}
}

func TestPrimaryOnlyUsesAlreadyUpgradedReplica(t *testing.T) {
	nodes := fleetOf(1, 1, 2)
	nodes[2].Running = "v2" // n2 already on target
	ops := &fakeOps{}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 1}

	if _, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{PrimaryOnly: true})); err != nil {
		t.Fatal(err)
	}
	if ops.order[0] != "failover c0-s0-n0->c0-s0-n2" {
		t.Errorf("order: %v", ops.order)
	}
}

func TestNoCandidateIsPrimaryFailure(t *testing.T) {
	nodes := fleetOf(1, 1, 1)
	b := &Batch{Processor: &LeaderAware{Ops: &fakeOps{}}, Log: quiet, Workers: 1}
	res, _ := b.Run(context.Background(), Plan(nodes, &filter.Filter{PrimaryOnly: true}))
	if res.PrimaryFailures != 1 {
		t.Errorf("expected 1 primary failure, got %+v", res)
	}
}

func TestFailFastIsExact(t *testing.T) {
	nodes := fleetOf(1, 50, 1)
	fo := map[string]bool{}
	for i := 0; i < 50; i++ {
		fo[fmt.Sprintf("c0/s%03d", i)] = true // every failover fails
	}
	ops := &fakeOps{failOver: fo}

	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 1, MaxPrimaryFailures: 3}
	res, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{}))
	if !errors.Is(err, ErrFailFast) {
		t.Fatalf("expected ErrFailFast, got %v", err)
	}
	if got := ops.failovers.Load(); got != 3 {
		t.Errorf("failovers attempted = %d, want exactly 3", got)
	}
	if res.ShardsNotStarted != 47 {
		t.Errorf("not started = %d, want 47", res.ShardsNotStarted)
	}
}

func TestStopAt(t *testing.T) {
	nodes := fleetOf(2, 10, 1)
	ops := &fakeOps{}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 2, StopAt: time.Now().Add(-time.Second)}
	res, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{}))
	if err != nil {
		t.Fatal(err)
	}
	if res.ShardsDone != 0 || res.ShardsNotStarted != 20 || res.StopReason == "" {
		t.Errorf("res: %+v", res)
	}
}

func TestDryRunTouchesNothing(t *testing.T) {
	nodes := fleetOf(2, 5, 2)
	ops := &fakeOps{}
	lk := lock.NewMemory()
	lk.Steal("process/roll") // would block a real run
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Locker: lk, Log: quiet, Workers: 3, DryRun: true}
	if _, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{})); err != nil {
		t.Fatal(err)
	}
	if ops.restarts.Load()+ops.failovers.Load() != 0 {
		t.Error("dry run called NodeOps")
	}
}

func TestProcessLockHeld(t *testing.T) {
	lk := lock.NewMemory()
	lk.Steal("process/roll")
	b := &Batch{Processor: &LeaderAware{Ops: &fakeOps{}}, Locker: lk, Log: quiet, Workers: 1}
	if _, err := b.Run(context.Background(), Plan(fleetOf(1, 1, 1), &filter.Filter{})); !errors.Is(err, lock.ErrHeld) {
		t.Fatalf("expected ErrHeld, got %v", err)
	}
}

func TestProcessLockLostAborts(t *testing.T) {
	lk := lock.NewMemory()
	ops := &fakeOps{delay: 20 * time.Millisecond}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Locker: lk, Log: quiet, Workers: 1, LockValidateInterval: 10 * time.Millisecond}

	go func() {
		time.Sleep(30 * time.Millisecond)
		lk.Steal("process/roll")
	}()

	res, err := b.Run(context.Background(), Plan(fleetOf(1, 50, 1), &filter.Filter{}))
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("expected ErrLockLost, got %v", err)
	}
	if res.ShardsNotStarted == 0 {
		t.Error("expected unstarted shards after abort")
	}
}

func TestGuardSkipsShard(t *testing.T) {
	ops := &fakeOps{}
	g := GuardFunc(func(ctx context.Context, s *Shard) error {
		if s.Name == "s001" {
			return errors.New("node replacement in progress")
		}

		return nil
	})
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Guards: []Guard{g}, Log: quiet, Workers: 2}
	res, _ := b.Run(context.Background(), Plan(fleetOf(1, 3, 1), &filter.Filter{}))
	if len(res.ShardFailures) != 1 || ops.failovers.Load() != 2 {
		t.Errorf("res: %+v failovers=%d", res, ops.failovers.Load())
	}
}

func TestShardsPerKeyspace(t *testing.T) {
	shards := Plan(fleetOf(3, 10, 1), &filter.Filter{ShardsPerKeyspace: 1})
	if len(shards) != 3 {
		t.Errorf("canary plan has %d shards, want 3", len(shards))
	}
}

func TestWindow(t *testing.T) {
	w, err := ParseWindow("22:00-02:00", "UTC")
	if err != nil {
		t.Fatal(err)
	}
	at := func(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }
	for _, c := range []struct {
		t    time.Time
		want bool
	}{{at(23, 0), true}, {at(1, 59), true}, {at(2, 0), false}, {at(12, 0), false}} {
		if got := w.Contains(c.t); got != c.want {
			t.Errorf("Contains(%v) = %v", c.t, got)
		}
	}

	if _, err := ParseWindow("6-11", ""); err == nil {
		t.Error("expected parse error")
	}
}

func TestDrainLetsInflightFinish(t *testing.T) {
	nodes := fleetOf(1, 40, 1)
	ops := &fakeOps{delay: 30 * time.Millisecond}
	b := &Batch{Processor: &LeaderAware{Ops: ops}, Log: quiet, Workers: 4}

	go func() {
		time.Sleep(45 * time.Millisecond)
		b.Drain(errors.New("operator pressed ctrl-c"))
	}()

	res, err := b.Run(context.Background(), Plan(nodes, &filter.Filter{}))
	if err == nil || res.Cause == nil {
		t.Fatalf("expected a drain cause, got err=%v", err)
	}
	if res.ShardsNotStarted == 0 {
		t.Error("expected shards not started after drain")
	}
	if len(res.NodeFailures)+len(res.ShardFailures) != 0 {
		t.Errorf("a drain is not a failure: %+v", res)
	}
	// every restart that started also completed (no half-done nodes)
	done := 0
	for _, n := range nodes {
		if n.Running == "v2" {
			done++
		}
	}
	if int64(done) != ops.restarts.Load() {
		t.Errorf("restarts started=%d completed=%d", ops.restarts.Load(), done)
	}
}

func TestDrainBeforeRun(t *testing.T) {
	b := &Batch{Processor: &LeaderAware{Ops: &fakeOps{}}, Log: quiet, Workers: 1}
	b.Drain(errors.New("early"))
	res, _ := b.Run(context.Background(), Plan(fleetOf(1, 3, 1), &filter.Filter{}))
	if res.ShardsNotStarted != 3 {
		t.Errorf("res: %+v", res)
	}
}
