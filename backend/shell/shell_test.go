package shell

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/roll"
)

func TestShellBackendEndToEnd(t *testing.T) {
	dir := t.TempDir()
	inv := filepath.Join(dir, "inventory.json")
	journal := filepath.Join(dir, "journal.log")

	if err := os.WriteFile(inv, []byte(`[
	 {"name":"a1","host":"db1","keyspace":"users","shard":"-80","cell":"az1","role":"primary","state":"RestartWait","running":"8.0.39","target":"8.0.40"},
	 {"name":"a2","host":"db2","keyspace":"users","shard":"-80","cell":"az2","role":"replica","state":"RestartWait","running":"8.0.39","target":"8.0.40"}
	]`), 0o644); err != nil {
		t.Fatal(err)
	}

	f := NewFactory()
	f.cfg = Config{
		Inventory: "cat " + inv,
		Restart:   "echo restart {{.Node.Host}} $HD_TARGET >> " + journal,
		Failover:  "echo failover {{.From.Host}} $HD_TO_HOST >> " + journal,
		LockDir:   filepath.Join(dir, "locks"),
		Timeout:   10 * time.Second,
	}
	quiet := log.New(io.Discard, "", 0)

	be, err := f.Open(context.Background(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := be.Inventory().Nodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	b := &roll.Batch{Processor: be.Processor(), Locker: be.Locker(), Log: quiet, Workers: 2}
	b.Processor.(*roll.LeaderAware).Jitter = 0
	if _, err := b.Run(context.Background(), roll.Plan(nodes, &filter.Filter{})); err != nil {
		t.Fatal(err)
	}

	got, _ := os.ReadFile(journal)
	want := "restart db2 8.0.40\nfailover db1 db2\nrestart db1 8.0.40\n"
	if string(got) != want {
		t.Errorf("journal:\n%s\nwant:\n%s", got, want)
	}

	left, _ := os.ReadDir(f.cfg.LockDir)
	if len(left) != 0 {
		t.Errorf("lock files left behind: %v", left)
	}
}

func TestShellCommandFailureSurfacesStderr(t *testing.T) {
	b := &shellBackend{cfg: Config{Timeout: time.Second}}
	_, err := b.run(context.Background(), "echo nope >&2; exit 3", nil)
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("got %v", err)
	}
}
