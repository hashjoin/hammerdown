// Package report renders fleet state the way operators read it: plain
// fixed-width ASCII that survives ssh, tmux, grep and awk.
//
//	$ hammerdown report | egrep 'Keyspace:Build|:v20' | grep -v 100.00
package report

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/roll"
)

// StateCount - nodes of a keyspace in a deploy state.
type StateCount struct {
	State    fleet.State `json:"state"`
	Keyspace string      `json:"keyspace"`
	Count    int         `json:"count"`
}

// BuildRow - per keyspace/build counts. DonePct = Running/Target.
type BuildRow struct {
	Keyspace string  `json:"keyspace"`
	Build    string  `json:"build"`
	Deployed int     `json:"deployed"`
	Running  int     `json:"running"`
	Target   int     `json:"target"`
	DonePct  float64 `json:"done_pct"`
}

// Summary is the full report (also emitted as JSON with --json).
type Summary struct {
	Nodes     int                           `json:"nodes"`
	Shards    int                           `json:"shards"`
	Keyspaces []string                      `json:"keyspaces"`
	Builds    []string                      `json:"builds"`
	States    []StateCount                  `json:"states"`
	Rows      []BuildRow                    `json:"rows"`
	Matrix    map[string]map[string]float64 `json:"running_pct"` // keyspace -> build -> % of keyspace running it
}

// Summarize computes a Summary.
func Summarize(nodes []*fleet.Node) *Summary {
	s := &Summary{Nodes: len(nodes), Matrix: map[string]map[string]float64{}}

	type key struct{ a, b string }
	states := map[key]int{}
	rows := map[key]*BuildRow{}
	keyspaceTotal := map[string]int{}
	shards := map[string]bool{}
	builds := map[string]bool{}

	row := func(keyspace, build string) *BuildRow {
		k := key{keyspace, build}
		if rows[k] == nil {
			rows[k] = &BuildRow{Keyspace: keyspace, Build: build}
			builds[build] = true
		}

		return rows[k]
	}

	for _, n := range nodes {
		states[key{string(n.State), n.Keyspace}]++
		keyspaceTotal[n.Keyspace]++
		shards[n.ShardKey()] = true

		if n.Deployed != "" {
			row(n.Keyspace, n.Deployed).Deployed++
		}
		if n.Running != "" {
			row(n.Keyspace, n.Running).Running++
		}
		if n.Target != "" {
			row(n.Keyspace, n.Target).Target++
		}
	}

	s.Shards = len(shards)
	for c := range keyspaceTotal {
		s.Keyspaces = append(s.Keyspaces, c)
	}
	sort.Strings(s.Keyspaces)
	for b := range builds {
		s.Builds = append(s.Builds, b)
	}
	sort.Slice(s.Builds, func(i, j int) bool { return naturalLess(s.Builds[i], s.Builds[j]) })

	for k, c := range states {
		s.States = append(s.States, StateCount{State: fleet.State(k.a), Keyspace: k.b, Count: c})
	}
	sort.Slice(s.States, func(i, j int) bool {
		if s.States[i].State != s.States[j].State {
			return s.States[i].State < s.States[j].State
		}

		return s.States[i].Keyspace < s.States[j].Keyspace
	})

	for _, r := range rows {
		if r.Target > 0 {
			r.DonePct = 100 * float64(r.Running) / float64(r.Target)
		}
		s.Rows = append(s.Rows, *r)

		if s.Matrix[r.Keyspace] == nil {
			s.Matrix[r.Keyspace] = map[string]float64{}
		}
		s.Matrix[r.Keyspace][r.Build] = 100 * float64(r.Running) / float64(keyspaceTotal[r.Keyspace])
	}
	sort.Slice(s.Rows, func(i, j int) bool {
		if s.Rows[i].Keyspace != s.Rows[j].Keyspace {
			return s.Rows[i].Keyspace < s.Rows[j].Keyspace
		}

		return naturalLess(s.Rows[i].Build, s.Rows[j].Build)
	})

	return s
}

// WriteText prints the three classic sections.
func (s *Summary) WriteText(w io.Writer) {
	fmt.Fprintf(w, "--------- Fleet: %d node(s), %d shard(s), %d keyspace(s) ---------\n\n", s.Nodes, s.Shards, len(s.Keyspaces))

	fmt.Fprintf(w, "--------- Node Counts By State ---------\n")
	for _, sc := range s.States {
		fmt.Fprintf(w, "%-15s %-20s %6d\n", sc.State, sc.Keyspace, sc.Count)
	}

	fmt.Fprintf(w, "\n\n%27s|%9s|%9s|%9s|%9s|\n", "Keyspace:Build", "Deployed", "Running", "Target", "Done %")
	fmt.Fprintf(w, "%s|%s|%s|%s|%s|\n", strings.Repeat("-", 27), dash(9), dash(9), dash(9), dash(9))
	for _, r := range s.Rows {
		fmt.Fprintf(w, "%27s|%9d|%9d|%9d|%9.2f|\n", r.Keyspace+":"+r.Build, r.Deployed, r.Running, r.Target, r.DonePct)
	}

	fmt.Fprintf(w, "\n\n%20s|", "Keyspace")
	for _, b := range s.Builds {
		fmt.Fprintf(w, "%12s|", b)
	}
	fmt.Fprintf(w, "\n%s|", dash(20))
	for range s.Builds {
		fmt.Fprintf(w, "%s|", dash(12))
	}
	fmt.Fprintln(w)
	for _, c := range s.Keyspaces {
		fmt.Fprintf(w, "%20s|", c)
		for _, b := range s.Builds {
			fmt.Fprintf(w, "%11.2f%%|", s.Matrix[c][b])
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w)
}

// WritePlan prints what a roll is about to do.
func WritePlan(w io.Writer, shards []*roll.Shard, workers, keyspaceWorkers int, dryRun, verbose bool) {
	st := roll.Stats(shards)
	fmt.Fprintf(w, "--------- Plan (dry-run=%v) ---------\n", dryRun)
	fmt.Fprintf(w, "keyspaces: %d | shards: %d | nodes: %d (%d primaries) | workers: %d",
		st.Keyspaces, st.Shards, st.Nodes, st.Primaries, workers)
	if keyspaceWorkers > 0 {
		fmt.Fprintf(w, " | per-keyspace cap: %d", keyspaceWorkers)
	}
	fmt.Fprintln(w)

	perKeyspace := map[string][2]int{}
	var keyspaces []string
	for _, s := range shards {
		v, ok := perKeyspace[s.Keyspace]
		if !ok {
			keyspaces = append(keyspaces, s.Keyspace)
		}
		v[0]++
		v[1] += len(s.Targets)
		perKeyspace[s.Keyspace] = v
	}
	for _, c := range keyspaces {
		fmt.Fprintf(w, "  %-20s %6d shard(s) %7d node(s)\n", c, perKeyspace[c][0], perKeyspace[c][1])
	}

	if verbose {
		for _, s := range shards {
			for _, n := range s.Targets {
				fmt.Fprintf(w, "    %-8s %-40s %s -> %s\n", n.Role, n.String(), n.Running, n.Target)
			}
		}
	}
	fmt.Fprintln(w)
}

// WriteResult prints the end-of-roll summary and every failure.
func WriteResult(w io.Writer, r *roll.Result) {
	fmt.Fprintf(w, "\n--------- Roll Result ---------\n")
	fmt.Fprintf(w, "elapsed:            %s\n", r.Finished.Sub(r.Started).Round(time.Millisecond))
	fmt.Fprintf(w, "shards completed:   %d / %d\n", r.ShardsDone-len(r.Interrupted), r.Planned.Shards)
	fmt.Fprintf(w, "shards interrupted: %d\n", len(r.Interrupted))
	fmt.Fprintf(w, "shards not started: %d\n", r.ShardsNotStarted)
	fmt.Fprintf(w, "primary failures:   %d\n", r.PrimaryFailures)
	fmt.Fprintf(w, "replica failures:   %d\n", r.ReplicaFailures)
	if r.StopReason != "" {
		fmt.Fprintf(w, "stopped early:      %s\n", r.StopReason)
	}
	if r.Cause != nil {
		fmt.Fprintf(w, "aborted:            %v\n", r.Cause)
	}

	if len(r.Interrupted)+r.ShardsNotStarted > 0 {
		fmt.Fprintf(w, "\nRe-run the same command to resume: finished nodes are OK and drop out of the plan.\n")
	}

	if len(r.NodeFailures)+len(r.ShardFailures) == 0 {
		return
	}

	fmt.Fprintf(w, "\n--------- Failures ---------\n")
	for _, f := range r.ShardFailures {
		fmt.Fprintf(w, "SHARD %-28s %v\n", f.Shard, f.Err)
	}
	for _, f := range r.NodeFailures {
		fmt.Fprintf(w, "NODE  %-28s %v\n", f.Shard, f.Err)
	}
}

func dash(n int) string { return strings.Repeat("-", n) }

// naturalLess orders "v9.0" before "v10.0" and "849" before "1001".
func naturalLess(a, b string) bool {
	for a != "" && b != "" {
		ca, cb := chunk(a), chunk(b)
		a, b = a[len(ca):], b[len(cb):]
		if ca == cb {
			continue
		}
		da, db := unicode.IsDigit(rune(ca[0])), unicode.IsDigit(rune(cb[0]))
		if da && db {
			ta, tb := strings.TrimLeft(ca, "0"), strings.TrimLeft(cb, "0")
			if len(ta) != len(tb) {
				return len(ta) < len(tb)
			}

			return ta < tb
		}

		return ca < cb
	}

	return len(a) < len(b)
}

func chunk(s string) string {
	digit := unicode.IsDigit(rune(s[0]))
	i := 1
	for i < len(s) && unicode.IsDigit(rune(s[i])) == digit {
		i++
	}

	return s[:i]
}
