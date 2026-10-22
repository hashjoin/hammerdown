package report

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hashjoin/hammerdown/fleet"
)

func TestSummarize(t *testing.T) {
	nodes := []*fleet.Node{
		{Keyspace: "users", Shard: "-80", State: fleet.StateOK, Deployed: "v20", Running: "v20", Target: "v20"},
		{Keyspace: "users", Shard: "-80", State: fleet.StateRestartWait, Deployed: "v20", Running: "v19", Target: "v20"},
		{Keyspace: "users", Shard: "80-", State: fleet.StateRestartWait, Deployed: "v20", Running: "v19", Target: "v20"},
		{Keyspace: "users", Shard: "80-", State: fleet.StateRestartWait, Deployed: "v20", Running: "v19", Target: "v20"},
	}
	s := Summarize(nodes)

	if s.Nodes != 4 || s.Shards != 2 || len(s.Keyspaces) != 1 {
		t.Fatalf("summary: %+v", s)
	}
	var v20 BuildRow
	for _, r := range s.Rows {
		if r.Build == "v20" {
			v20 = r
		}
	}
	if v20.Target != 4 || v20.Running != 1 || v20.DonePct != 25 {
		t.Errorf("v20 row: %+v", v20)
	}
	if s.Matrix["users"]["v19"] != 75 {
		t.Errorf("matrix: %+v", s.Matrix)
	}

	var buf bytes.Buffer
	s.WriteText(&buf)
	if !strings.Contains(buf.String(), "users:v20|        4|        1|        4|    25.00|") {
		t.Errorf("text:\n%s", buf.String())
	}
}

func TestNaturalLess(t *testing.T) {
	if !naturalLess("v9.0.1", "v10.0.0") || naturalLess("v10.0.0", "v9.0.1") || !naturalLess("849", "1001") {
		t.Error("natural ordering broken")
	}
}
