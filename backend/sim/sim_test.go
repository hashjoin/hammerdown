package sim

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/roll"
)

func TestGenerate(t *testing.T) {
	f := Generate(42)
	if len(f.Nodes) != 11000 {
		t.Fatalf("nodes = %d, want 11000", len(f.Nodes))
	}

	primaries := map[string]int{}
	names := map[string]bool{}
	for _, n := range f.Nodes {
		if names[n.Name] {
			t.Fatalf("duplicate name %s", n.Name)
		}
		names[n.Name] = true
		if n.IsPrimary() {
			primaries[n.ShardKey()]++
		}
	}
	if len(primaries) != 2829 {
		t.Errorf("shards = %d, want 2829", len(primaries))
	}
	for k, c := range primaries {
		if c != 1 {
			t.Errorf("%s has %d primaries", k, c)
		}
	}
}

func TestKeyRange(t *testing.T) {
	if got := keyRange(0, 2) + " " + keyRange(1, 2); got != "-8000 8000-" {
		t.Errorf("got %q", got)
	}
}

func TestStageAndRoll(t *testing.T) {
	state := filepath.Join(t.TempDir(), "fleet.json")
	fac := NewFactory()
	fac.cfg = Config{State: state, Seed: 7, Latency: time.Millisecond}
	quiet := log.New(io.Discard, "", 0)

	be, err := fac.Open(context.Background(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	limit, _ := filter.ParseSpec("users/ALL|config")
	scope := &filter.Filter{Limit: limit}
	staged := be.(*simBackend).Fleet().Stage(scope, "v20.0.1")
	if staged == 0 {
		t.Fatal("nothing staged")
	}

	nodes, _ := be.Inventory().Nodes(context.Background())
	shards := roll.Plan(nodes, scope)
	b := &roll.Batch{Processor: be.Processor(), Locker: be.Locker(), Log: quiet, Workers: 32}
	if _, err := b.Run(context.Background(), shards); err != nil {
		t.Fatal(err)
	}
	if err := be.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloaded, err := Load(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range reloaded.Nodes {
		inScope, _ := scope.InScope(n)
		if inScope && n.State != fleet.StateDrained && n.Running != "v20.0.1" {
			t.Fatalf("%s still on %s", n, n.Running)
		}
		if !inScope && n.Running != StartBuild {
			t.Fatalf("%s touched outside scope", n)
		}
	}
}
