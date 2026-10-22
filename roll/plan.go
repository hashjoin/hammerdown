package roll

import (
	"sort"

	"github.com/hashjoin/hammerdown/filter"
	"github.com/hashjoin/hammerdown/fleet"
)

// Shard is the unit of work: one worker owns one shard at a time.
type Shard struct {
	Keyspace string
	Name     string
	// Targets are the nodes selected for restart (filter matched).
	Targets []*fleet.Node
	// Members are all nodes of the shard, selected or not. Processors use
	// them to find failover candidates that are already on the target build.
	Members []*fleet.Node
}

// Key is "keyspace/shard".
func (s *Shard) Key() string { return s.Keyspace + "/" + s.Name }

// Plan groups the nodes selected by f into shards, sorted by keyspace then
// shard. Within a shard, targets are ordered replicas first, primary last.
func Plan(nodes []*fleet.Node, f *filter.Filter) []*Shard {
	byKey := map[string]*Shard{}

	for _, n := range nodes {
		s, ok := byKey[n.ShardKey()]
		if !ok {
			s = &Shard{Keyspace: n.Keyspace, Name: n.Shard}
			byKey[n.ShardKey()] = s
		}
		s.Members = append(s.Members, n)

		if ok, _ := f.Match(n); ok {
			s.Targets = append(s.Targets, n)
		}
	}

	var shards []*Shard
	for _, s := range byKey {
		if len(s.Targets) == 0 {
			continue
		}
		sort.SliceStable(s.Targets, func(i, j int) bool {
			pi, pj := s.Targets[i].IsPrimary(), s.Targets[j].IsPrimary()
			if pi != pj {
				return !pi
			}

			return s.Targets[i].Name < s.Targets[j].Name
		})
		shards = append(shards, s)
	}

	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Keyspace != shards[j].Keyspace {
			return shards[i].Keyspace < shards[j].Keyspace
		}

		return shards[i].Name < shards[j].Name
	})

	if f.ShardsPerKeyspace > 0 {
		var capped []*Shard
		seen := map[string]int{}
		for _, s := range shards {
			if seen[s.Keyspace] < f.ShardsPerKeyspace {
				capped = append(capped, s)
				seen[s.Keyspace]++
			}
		}
		shards = capped
	}

	return shards
}

// PlanStats summarizes a plan.
type PlanStats struct {
	Keyspaces, Shards, Nodes, Primaries int
}

// Stats counts what a plan will touch.
func Stats(shards []*Shard) PlanStats {
	var st PlanStats
	keyspaces := map[string]bool{}
	for _, s := range shards {
		keyspaces[s.Keyspace] = true
		st.Shards++
		for _, n := range s.Targets {
			st.Nodes++
			if n.IsPrimary() {
				st.Primaries++
			}
		}
	}
	st.Keyspaces = len(keyspaces)

	return st
}
