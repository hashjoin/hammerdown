// Package sim is the default backend: a simulated 11,000-node, 2,829-shard
// database fleet that behaves enough like the real thing to demo and test
// every hammerdown feature on a laptop - workers, filters, failovers,
// fail-fast, stop-at, blackout windows - in seconds.
//
// State lives in a JSON file (default .hammerdown/sim-fleet.json) so you can
// `report`, `roll` a slice, and `report` again, exactly like on a real fleet.
//
//	hammerdown sim init                       # build the fleet on v19.0.4
//	hammerdown sim stage --to v20.0.1         # stage a new build -> RestartWait
//	hammerdown report
//	hammerdown roll --limit 'users/ALL' --workers 16 --dry-run=false --yes
package sim

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashjoin/hammerdown/backend"
	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
	"github.com/hashjoin/hammerdown/roll"
)

// DefaultState is where the simulated fleet is persisted.
const DefaultState = ".hammerdown/sim-fleet.json"

// StartBuild is the build a freshly initialized fleet runs.
const StartBuild = "v19.0.4"

// Topology: keyspace, shards, nodes per shard. 2,829 shards, 11,000 nodes.
var Topology = []struct {
	Keyspace      string
	Shards, Nodes int
}{
	{"messages", 1088, 4},
	{"threads", 540, 4},
	{"teams", 360, 4},
	{"users", 256, 4},
	{"files", 128, 4},
	{"channels", 128, 3},
	{"search", 64, 3},
	{"presence", 64, 4},
	{"audit", 64, 2},
	{"sessions", 32, 4},
	{"notifications", 32, 4},
	{"billing", 16, 4},
	{"webhooks", 16, 4},
	{"reactions", 16, 4},
	{"auth", 8, 4},
	{"outbox", 8, 4},
	{"loadtest", 5, 4},
	{"flags", 2, 4},
	{"canary", 1, 4},
	{"config", 1, 8},
}

var cells = []string{"az1", "az2", "az3"}

// Fleet is the persisted simulated fleet.
type Fleet struct {
	Seed  int64         `json:"seed"`
	Nodes []*fleet.Node `json:"nodes"`
}

// Generate builds the fleet deterministically from seed.
func Generate(seed int64) *Fleet {
	r := rand.New(rand.NewSource(seed)) // #nosec G404 - simulation
	f := &Fleet{Seed: seed}
	uid := 100

	for _, t := range Topology {
		for i := 0; i < t.Shards; i++ {
			shard := keyRange(i, t.Shards)
			for k := 0; k < t.Nodes; k++ {
				uid++
				cell := cells[(i+k)%len(cells)]
				n := &fleet.Node{
					Name:     fmt.Sprintf("%s-%010d", cell, uid),
					Host:     fmt.Sprintf("db-%s-%04d-%d.%s.sim", t.Keyspace, i, k, cell),
					Keyspace: t.Keyspace,
					Shard:    shard,
					Cell:     cell,
					Role:     roleFor(k, t.Nodes),
					State:    fleet.StateOK,
					Deployed: StartBuild,
					Running:  StartBuild,
					Target:   StartBuild,
				}
				if n.Role != fleet.Primary && r.Float64() < 0.003 {
					n.State = fleet.StateDrained
				}
				f.Nodes = append(f.Nodes, n)
			}
		}
	}

	return f
}

func roleFor(k, nodes int) fleet.Role {
	switch {
	case k == 0:
		return fleet.Primary
	case nodes >= 4 && k >= nodes-nodes/4:
		return fleet.ReadOnly
	default:
		return fleet.Replica
	}
}

// keyRange names shard i of n Vitess-style: "-0f0f", "0f0f-1e1e", ..., "f0f0-".
func keyRange(i, n int) string {
	if n == 1 {
		return "0"
	}
	lo, hi := i*65536/n, (i+1)*65536/n
	a, b := fmt.Sprintf("%04x", lo), fmt.Sprintf("%04x", hi)
	if i == 0 {
		a = ""
	}
	if i == n-1 {
		b = ""
	}

	return a + "-" + b
}

// Load reads a fleet from path.
func Load(path string) (*Fleet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f Fleet
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return &f, nil
}

// Save writes the fleet atomically.
func (f *Fleet) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

// Stage is the sim's "deploy": put build `to` on disk for every in-scope,
// non-drained node and mark nodes that aren't running it RestartWait.
func (f *Fleet) Stage(flt *filter.Filter, to string) (staged int) {
	for _, n := range f.Nodes {
		if ok, _ := flt.InScope(n); !ok || n.State == fleet.StateDrained {
			continue
		}
		n.Deployed, n.Target = to, to
		if n.Running != to {
			n.State = fleet.StateRestartWait
			staged++
		} else {
			n.State = fleet.StateOK
		}
	}

	return staged
}

// Config holds the sim flags.
type Config struct {
	State    string
	Seed     int64
	Latency  time.Duration
	FailRate float64
	Cursed   int
}

// Factory is the sim backend.Factory.
type Factory struct{ cfg Config }

// NewFactory returns the sim factory.
func NewFactory() *Factory { return &Factory{} }

// Name implements backend.Factory.
func (*Factory) Name() string { return "sim" }

// Describe implements backend.Factory.
func (*Factory) Describe() string {
	return "simulated 11,000-node / 2,829-shard fleet persisted to a JSON file (default)"
}

// RegisterFlags implements backend.Factory.
func (f *Factory) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&f.cfg.State, "sim-state", DefaultState, "sim: fleet state file")
	fs.Int64Var(&f.cfg.Seed, "sim-seed", 42, "sim: random seed (fleet layout, latency, failures)")
	fs.DurationVar(&f.cfg.Latency, "sim-latency", 50*time.Millisecond, "sim: mean restart/failover latency")
	fs.Float64Var(&f.cfg.FailRate, "sim-fail-rate", 0, "sim: probability any single restart/failover fails")
	fs.IntVar(&f.cfg.Cursed, "sim-cursed", 0, "sim: number of shards whose failover always fails")
}

// Config returns the parsed flags (used by the `sim` subcommand).
func (f *Factory) Config() Config { return f.cfg }

// Open implements backend.Factory.
func (f *Factory) Open(_ context.Context, log roll.Logger) (backend.Backend, error) {
	fl, err := Load(f.cfg.State)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("sim: %s not found, generating a fresh fleet (seed=%d)", f.cfg.State, f.cfg.Seed)
		fl = Generate(f.cfg.Seed)
	} else if err != nil {
		return nil, err
	}

	b := &simBackend{
		cfg:    f.cfg,
		fleet:  fl,
		rnd:    rand.New(rand.NewSource(f.cfg.Seed + time.Now().UnixNano())), // #nosec G404
		locker: lock.NewMemory(),
		cursed: pickCursed(fl, f.cfg.Seed, f.cfg.Cursed),
	}
	if len(b.cursed) > 0 {
		keys := make([]string, 0, len(b.cursed))
		for k := range b.cursed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		log.Printf("sim: cursed shards (failover always fails): %v", keys)
	}

	return b, nil
}

func pickCursed(f *Fleet, seed int64, n int) map[string]bool {
	out := map[string]bool{}
	if n <= 0 {
		return out
	}
	var keys []string
	seen := map[string]bool{}
	for _, node := range f.Nodes {
		if !seen[node.ShardKey()] {
			seen[node.ShardKey()] = true
			keys = append(keys, node.ShardKey())
		}
	}
	r := rand.New(rand.NewSource(seed)) // #nosec G404
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for i := 0; i < n && i < len(keys); i++ {
		out[keys[i]] = true
	}

	return out
}

type simBackend struct {
	cfg    Config
	fleet  *Fleet
	locker *lock.Memory
	cursed map[string]bool

	mu  sync.Mutex
	rnd *rand.Rand
}

func (b *simBackend) Inventory() fleet.Inventory {
	return fleet.InventoryFunc(func(context.Context) ([]*fleet.Node, error) { return b.fleet.Nodes, nil })
}

func (b *simBackend) Processor() roll.Processor { return &roll.LeaderAware{Ops: b} }

func (b *simBackend) Locker() lock.Locker { return b.locker }

func (b *simBackend) Guards() []roll.Guard { return nil }

func (b *simBackend) Close(context.Context) error { return b.fleet.Save(b.cfg.State) }

// Fleet exposes the underlying fleet (for the `sim` subcommand).
func (b *simBackend) Fleet() *Fleet { return b.fleet }

func (b *simBackend) roll() (latency time.Duration, fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cfg.Latency > 0 {
		latency = b.cfg.Latency/2 + time.Duration(b.rnd.Int63n(int64(b.cfg.Latency)))
	}

	return latency, b.rnd.Float64() < b.cfg.FailRate
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// Restart implements roll.NodeOps.
func (b *simBackend) Restart(ctx context.Context, n *fleet.Node) error {
	latency, fail := b.roll()
	if err := sleep(ctx, latency); err != nil {
		return err
	}
	if fail {
		return fmt.Errorf("%s did not report SERVING within 60s after restart (simulated)", n.Host)
	}
	n.Running, n.State = n.Target, fleet.StateOK

	return nil
}

// Failover implements roll.NodeOps.
func (b *simBackend) Failover(ctx context.Context, s *roll.Shard, from, to *fleet.Node) error {
	latency, fail := b.roll()
	if err := sleep(ctx, latency); err != nil {
		return err
	}
	if b.cursed[s.Key()] {
		return fmt.Errorf("planned reparent: %s replication position did not catch up within 30s (simulated cursed shard)", to.Host)
	}
	if fail {
		return errors.New("planned reparent: lost connection to topo server (simulated)")
	}
	from.Role, to.Role = fleet.Replica, fleet.Primary

	return nil
}
