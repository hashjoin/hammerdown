package roll_test

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/roll"
)

// ops is the smallest possible roll.NodeOps: it "restarts" and "fails over"
// instantly. A real backend talks to your infrastructure here.
type ops struct{}

func (ops) Restart(_ context.Context, n *fleet.Node) error {
	n.Running, n.State = n.Target, fleet.StateOK

	return nil
}

func (ops) Failover(_ context.Context, _ *roll.Shard, from, to *fleet.Node) error {
	from.Role, to.Role = fleet.Replica, fleet.Primary

	return nil
}

// Roll two shards with the bundled LeaderAware processor: replicas first,
// planned failover, then the old primary.
func Example() {
	var nodes []*fleet.Node
	for _, shard := range []string{"-80", "80-"} {
		for i, role := range []fleet.Role{fleet.Primary, fleet.Replica, fleet.Replica} {
			nodes = append(nodes, &fleet.Node{
				Name: fmt.Sprintf("users%s-%d", shard, i), Host: fmt.Sprintf("db%s-%d", shard, i),
				Keyspace: "users", Shard: shard, Role: role,
				State: fleet.StateRestartWait, Running: "v1", Target: "v2",
			})
		}
	}

	limit, _ := filter.ParseSpec("users/ALL")
	shards := roll.Plan(nodes, &filter.Filter{Limit: limit})

	b := &roll.Batch{
		Processor:          &roll.LeaderAware{Ops: ops{}},
		Log:                log.New(io.Discard, "", 0),
		Workers:            2,
		MaxPrimaryFailures: 1,
	}

	res, err := b.Run(context.Background(), shards)
	if err != nil {
		fmt.Println("roll failed:", err)

		return
	}

	upgraded := 0
	for _, n := range nodes {
		if n.Running == "v2" {
			upgraded++
		}
	}
	fmt.Printf("shards: %d, nodes upgraded: %d, failures: %d\n",
		res.ShardsDone, upgraded, len(res.NodeFailures)+len(res.ShardFailures))
	// Output: shards: 2, nodes upgraded: 6, failures: 0
}
