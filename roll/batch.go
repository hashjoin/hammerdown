// Package roll is the scheduler: it fans a plan of shards out over a fixed
// pool of workers and keeps its foot near the brake the whole time.
//
// Guarantees (the stuff you want at 3 a.m.):
//
//   - at most Workers shards are in flight, fleet-wide; optionally at most
//     KeyspaceWorkers per keyspace;
//   - one worker owns a shard end to end, under a shard lock;
//   - with a Refresher, the shard is re-read under that lock and skipped
//     untouched if it no longer matches the plan (ErrStale): a plan is a
//     snapshot, and acting on a stale one restarts primaries;
//   - the whole batch runs under a process lock that is re-validated on a
//     ticker; losing it aborts the batch;
//   - fail-fast is exact: the failure that crosses MaxPrimaryFailures /
//     MaxReplicaFailures drains the batch before Fail() returns, so no
//     worker starts another node operation after the threshold is hit;
//   - draining (fail-fast, lost process lock, first Ctrl-C) never kills an
//     in-flight node operation: it finishes, its shard lock is released, and
//     nothing new starts. Only canceling the parent context aborts in-flight
//     work (second Ctrl-C);
//   - StopAt and the Blackout window stop NEW shards from starting;
//   - DryRun takes no locks and the bundled processors touch nothing.
package roll

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
)

// Logger is the only logging dependency. *log.Logger satisfies it.
type Logger interface {
	Printf(format string, v ...any)
}

// Processor restarts the targets of one shard. It is THE extension point:
// the bundled LeaderAware processor covers primary/replica systems, but you
// can implement anything shard-shaped here (config pushes, kernel patches,
// MySQL minor upgrades, cert rotation ...).
//
// Contract:
//   - call job.Stopped() before every node operation and return when it
//     reports the batch is draining;
//   - pass ctx to node operations; it is only canceled on a hard abort;
//   - report node failures with job.Fail (wrap ErrPrimary for primary-side
//     failures so they count toward MaxPrimaryFailures);
//   - return an error only for shard-level problems;
//   - in job.DryRun, log what you would do and change nothing.
type Processor interface {
	ProcessShard(ctx context.Context, job *Job) error
}

// ProcessorFunc adapts a function to Processor.
type ProcessorFunc func(ctx context.Context, job *Job) error

// ProcessShard implements Processor.
func (f ProcessorFunc) ProcessShard(ctx context.Context, job *Job) error { return f(ctx, job) }

// Guard is a pre-flight check run before a shard is locked and processed:
// "is a node replacement in progress?", "is replication lag sane?",
// "is a backup running?". Returning an error skips the shard.
type Guard interface {
	CheckShard(ctx context.Context, s *Shard) error
}

// GuardFunc adapts a function to Guard.
type GuardFunc func(ctx context.Context, s *Shard) error

// CheckShard implements Guard.
func (f GuardFunc) CheckShard(ctx context.Context, s *Shard) error { return f(ctx, s) }

var (
	// ErrPrimary classifies a failure as primary-side (failover, promotion).
	ErrPrimary = errors.New("primary failure")
	// ErrReplica classifies a failure as replica-side (anything else).
	ErrReplica = errors.New("replica failure")

	// ErrFailFast - a failure threshold was crossed.
	ErrFailFast = errors.New("fail-fast threshold breached")
	// ErrLockLost - the process lock could not be re-validated.
	ErrLockLost = errors.New("process lock lost")
	// ErrFailures - the batch completed with failures.
	ErrFailures = errors.New("roll finished with failures")
)

// Batch is one roll across a plan.
type Batch struct {
	Processor Processor
	Locker    lock.Locker // nil = no locking (not recommended outside demos)
	Refresher Refresher   // nil = trust the plan (not recommended outside demos)
	Guards    []Guard
	Log       Logger

	// Workers - max shards in flight across the whole fleet. Required.
	Workers int
	// KeyspaceWorkers - max shards in flight per keyspace (0 = no cap).
	KeyspaceWorkers int

	MaxPrimaryFailures int // 0 = unlimited
	MaxReplicaFailures int // 0 = unlimited

	StopAt     time.Time // no new shards after this instant
	Blackout   *Window   // no new shards while inside this window
	DryRun     bool
	LockReason string

	// LockValidateInterval - how often the process lock is re-validated
	// (default 30s).
	LockValidateInterval time.Duration

	now func() time.Time

	mu           sync.Mutex
	stopCtx      context.Context
	stop         context.CancelCauseFunc
	pendingDrain error
	primaryFails int
	replicaFails int
	res          *Result
}

// Failure is one recorded failure.
type Failure struct {
	Shard string // keyspace/shard
	Node  string // node String(), or "" for shard-level
	Err   error
}

// Result is what a batch did.
type Result struct {
	Started, Finished time.Time
	Planned           PlanStats
	ShardsDone        int // processed (with or without failures)
	ShardsNotStarted  int // never started: stop-at, blackout, abort
	NodeFailures      []Failure
	ShardFailures     []Failure
	Interrupted       []Failure // shards left part-way by a drain; rerun to resume
	PrimaryFailures   int
	ReplicaFailures   int
	StopReason        string // why new work stopped early, if it did
	Cause             error  // why the batch was canceled, if it was
}

// Failed reports whether anything failed or the batch was aborted.
func (r *Result) Failed() bool {
	return len(r.NodeFailures) > 0 || len(r.ShardFailures) > 0 || r.Cause != nil
}

func (b *Batch) validate() error {
	switch {
	case b.Processor == nil:
		return errors.New("roll: Processor is required")
	case b.Log == nil:
		return errors.New("roll: Log is required")
	case b.Workers < 1:
		return errors.New("roll: Workers must be >= 1")
	case b.KeyspaceWorkers < 0:
		return errors.New("roll: KeyspaceWorkers must be >= 0")
	}

	return nil
}

func (b *Batch) clock() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

// Run executes the plan. It returns the Result and, if anything went wrong,
// an error wrapping ErrFailures / ErrFailFast / ErrLockLost / ctx cause.
func (b *Batch) Run(parent context.Context, shards []*Shard) (*Result, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	if b.LockReason == "" {
		b.LockReason = "hammerdown roll"
	}
	if b.LockValidateInterval <= 0 {
		b.LockValidateInterval = 30 * time.Second
	}

	b.res = &Result{Started: b.clock(), Planned: Stats(shards)}
	for _, s := range shards {
		if s.planned == nil { // built by hand, not by Plan
			s.planned = snapshot(s.Members)
		}
	}
	b.primaryFails, b.replicaFails = 0, 0

	// ctx (the parent) is handed to node operations and only dies on a hard
	// abort. stopCtx is canceled to drain: no new shards, no new node ops.
	ctx := parent
	stopCtx, stop := context.WithCancelCause(parent)
	defer stop(nil)

	b.mu.Lock()
	b.stopCtx, b.stop = stopCtx, stop
	if b.pendingDrain != nil {
		stop(b.pendingDrain)
	}
	b.mu.Unlock()

	// 1. process lock
	if b.DryRun || b.Locker == nil {
		b.Log.Printf("( dry-run=%v ) process lock: skipped", b.DryRun)
	} else {
		b.Log.Printf("acquiring process lock process/roll")
		pl, err := b.Locker.Lock(ctx, "process/roll", b.LockReason)
		if err != nil {
			return b.res, fmt.Errorf("process lock: %w", err)
		}
		defer func() {
			if err := pl.Unlock(context.Background()); err != nil {
				b.Log.Printf("WARNING: releasing process lock: %v", err)
			} else {
				b.Log.Printf("released process lock process/roll")
			}
		}()

		go b.watchProcessLock(stopCtx, pl)
	}

	// 2. fan out: one dispatcher per keyspace, one global pool of worker slots
	slots := make(chan int, b.Workers)
	for i := 1; i <= b.Workers; i++ {
		slots <- i
	}

	byKeyspace := map[string][]*Shard{}
	var keyspaces []string
	for _, s := range shards {
		if _, ok := byKeyspace[s.Keyspace]; !ok {
			keyspaces = append(keyspaces, s.Keyspace)
		}
		byKeyspace[s.Keyspace] = append(byKeyspace[s.Keyspace], s)
	}

	var wg sync.WaitGroup
	for _, c := range keyspaces {
		wg.Add(1)
		go func(keyspace string, shards []*Shard) {
			defer wg.Done()
			b.dispatch(stopCtx, ctx, keyspace, shards, slots, &wg)
		}(c, byKeyspace[c])
	}

	wg.Wait()

	b.res.Finished = b.clock()
	b.res.PrimaryFailures, b.res.ReplicaFailures = b.primaryFails, b.replicaFails
	if stopCtx.Err() != nil {
		b.res.Cause = context.Cause(stopCtx)
	}

	sortFailures(b.res.NodeFailures)
	sortFailures(b.res.ShardFailures)
	sortFailures(b.res.Interrupted)

	b.Log.Printf("( dry-run=%v ) DONE: %d shard(s) processed, %d interrupted, %d not started, %d node failure(s), %d shard failure(s)",
		b.DryRun, b.res.ShardsDone, len(b.res.Interrupted), b.res.ShardsNotStarted, len(b.res.NodeFailures), len(b.res.ShardFailures))

	switch {
	case b.res.Cause != nil:
		return b.res, fmt.Errorf("roll aborted: %w", b.res.Cause)
	case b.res.Failed():
		return b.res, ErrFailures
	}

	return b.res, nil
}

// Drain stops the batch gracefully: in-flight node operations finish, no new
// ones start. Safe to call from a signal handler, before or during Run.
func (b *Batch) Drain(cause error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.drainLocked(cause)
}

func (b *Batch) drainLocked(cause error) {
	if b.stop == nil {
		if b.pendingDrain == nil {
			b.pendingDrain = cause
		}

		return
	}
	b.stop(cause)
}

func (b *Batch) dispatch(ctx, opCtx context.Context, keyspace string, shards []*Shard, slots chan int, wg *sync.WaitGroup) {
	var keyspaceSem chan struct{}
	if b.KeyspaceWorkers > 0 {
		keyspaceSem = make(chan struct{}, b.KeyspaceWorkers)
	}

	b.Log.Printf("( dry-run=%v ) rolling %s: %d shard(s)", b.DryRun, keyspace, len(shards))

	notStarted := func(i int, reason string) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.res.ShardsNotStarted += len(shards) - i
		if reason != "" && b.res.StopReason == "" {
			b.res.StopReason = reason
		}
	}

	for i, s := range shards {
		if ctx.Err() != nil {
			notStarted(i, "")
			return
		}

		if stop, reason := b.timeToStop(); stop {
			b.Log.Printf("WARNING: not starting %s (%d remaining in %s): %s", s.Key(), len(shards)-i, keyspace, reason)
			notStarted(i, reason)

			return
		}

		if keyspaceSem != nil {
			select {
			case keyspaceSem <- struct{}{}:
			case <-ctx.Done():
				notStarted(i, "")
				return
			}
		}

		var slot int
		select {
		case slot = <-slots:
		case <-ctx.Done():
			if keyspaceSem != nil {
				<-keyspaceSem
			}
			notStarted(i, "")

			return
		}

		// the stop conditions may have flipped while we waited for a slot
		if stop, reason := b.timeToStop(); stop || ctx.Err() != nil {
			slots <- slot
			if keyspaceSem != nil {
				<-keyspaceSem
			}
			if stop {
				b.Log.Printf("WARNING: not starting %s (%d remaining in %s): %s", s.Key(), len(shards)-i, keyspace, reason)
			}
			notStarted(i, reason)

			return
		}

		wg.Add(1)
		go func(s *Shard, slot int) {
			defer func() {
				slots <- slot
				if keyspaceSem != nil {
					<-keyspaceSem
				}
				wg.Done()
			}()
			b.runShard(opCtx, s, slot)
		}(s, slot)
	}
}

func (b *Batch) runShard(ctx context.Context, s *Shard, worker int) {
	job := &Job{Shard: s, DryRun: b.DryRun, Worker: worker, batch: b, stopCtx: b.stopCtx}

	job.Logf("processing %s (%d node(s))", s.Key(), len(s.Targets))

	shardErr := func(err error) {
		job.Logf("ERROR: %v", err)
		b.mu.Lock()
		b.res.ShardFailures = append(b.res.ShardFailures, Failure{Shard: s.Key(), Err: err})
		b.mu.Unlock()
	}

	defer func() {
		b.mu.Lock()
		b.res.ShardsDone++
		b.mu.Unlock()
	}()

	for _, g := range b.Guards {
		if err := g.CheckShard(ctx, s); err != nil {
			shardErr(fmt.Errorf("guard: %w", err))

			return
		}
	}

	if !b.DryRun && b.Locker != nil {
		key := "shard/" + s.Key()
		l, err := b.Locker.Lock(ctx, key, b.LockReason)
		if err != nil {
			shardErr(fmt.Errorf("shard lock: %w", err))

			return
		}
		defer func() {
			if err := l.Unlock(context.Background()); err != nil {
				job.Logf("WARNING: releasing %s: %v", key, err)
			}
		}()
	}

	// Under the lock, before anything else: is this still the shard we
	// planned? (Dry runs read too; reading touches nothing.)
	if err := b.refresh(ctx, s); err != nil {
		shardErr(err)

		return
	}

	if err := b.Processor.ProcessShard(ctx, job); err != nil {
		if job.Stopped() == nil {
			shardErr(err)

			return
		}
		// the batch is draining: this shard didn't fail, it was told to stop
		job.Logf("stopped: %v", err)
		b.mu.Lock()
		b.res.Interrupted = append(b.res.Interrupted, Failure{Shard: s.Key(), Err: err})
		b.mu.Unlock()
	}
}

func (b *Batch) watchProcessLock(ctx context.Context, pl lock.Lock) {
	t := time.NewTicker(b.LockValidateInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := pl.Validate(ctx); err != nil {
				b.Log.Printf("ERROR: process lock validation failed: %v", err)
				b.Drain(fmt.Errorf("%w: %w", ErrLockLost, err))

				return
			}
		}
	}
}

func (b *Batch) timeToStop() (bool, string) {
	now := b.clock()
	if b.Blackout != nil && b.Blackout.Contains(now) {
		return true, "inside blackout window " + b.Blackout.String()
	}
	if !b.StopAt.IsZero() && !now.Before(b.StopAt) {
		return true, "reached --stop-at " + b.StopAt.Format(time.RFC3339)
	}

	return false, ""
}

// countFailure does fail-fast accounting under the batch mutex, draining
// the batch synchronously when a threshold is crossed.
func (b *Batch) countFailure(shard string, n *fleet.Node, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := ""
	if n != nil {
		name = n.String()
	}
	b.res.NodeFailures = append(b.res.NodeFailures, Failure{Shard: shard, Node: name, Err: err})

	if errors.Is(err, ErrPrimary) {
		b.primaryFails++
		if b.MaxPrimaryFailures > 0 && b.primaryFails == b.MaxPrimaryFailures {
			b.Log.Printf("FAIL-FAST: %d primary failure(s) reached --max-primary-failures=%d, stopping", b.primaryFails, b.MaxPrimaryFailures)
			b.drainLocked(fmt.Errorf("%w: %d primary failure(s)", ErrFailFast, b.primaryFails))
		}

		return
	}

	b.replicaFails++
	if b.MaxReplicaFailures > 0 && b.replicaFails == b.MaxReplicaFailures {
		b.Log.Printf("FAIL-FAST: %d replica failure(s) reached --max-replica-failures=%d, stopping", b.replicaFails, b.MaxReplicaFailures)
		b.drainLocked(fmt.Errorf("%w: %d replica failure(s)", ErrFailFast, b.replicaFails))
	}
}

func sortFailures(f []Failure) {
	sort.SliceStable(f, func(i, j int) bool {
		if f[i].Shard != f[j].Shard {
			return f[i].Shard < f[j].Shard
		}

		return f[i].Node < f[j].Node
	})
}

// Job is handed to the Processor for one shard.
type Job struct {
	Shard  *Shard
	DryRun bool
	Worker int // worker slot, 1..Workers

	batch   *Batch
	stopCtx context.Context
}

// Stopped returns nil while the batch is running and the drain cause once
// it is draining. Check it before every node operation.
func (j *Job) Stopped() error {
	if j.stopCtx.Err() != nil {
		return context.Cause(j.stopCtx)
	}

	return nil
}

// Logf logs with the dry-run / worker prefix, e.g.
// "( dry-run=false ) ( worker=07 ) ... restarting replica ...".
func (j *Job) Logf(format string, v ...any) {
	j.batch.Log.Printf("( dry-run=%v ) ( worker=%02d ) ... "+format, append([]any{j.DryRun, j.Worker}, v...)...)
}

// Fail records a node failure and does fail-fast accounting. Wrap err with
// ErrPrimary for primary-side failures; everything else counts as replica.
// If this failure crosses a threshold, the batch is draining before Fail
// returns, so the processor's next job.Stopped() check sees it.
func (j *Job) Fail(n *fleet.Node, err error) {
	j.Logf("ERROR: %v", err)
	j.batch.countFailure(j.Shard.Key(), n, err)
}
