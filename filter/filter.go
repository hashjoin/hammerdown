// Package filter decides which nodes a command touches.
//
// The workhorse is Spec, a compact "keyspace/shard" selector that operators
// can type at 3 a.m. without looking anything up:
//
//	users/ALL                    every shard of the users keyspace
//	users/-80,80-                two shards of users
//	users/ALL|teams/40-80        '|' separates entries
//	mess*/ALL                    shell-style globs on keyspace ...
//	teams/4*                     ... and on shard
//	teams                        a bare keyspace means ALL shards
//
// A Filter combines a limit Spec, a skip Spec and a handful of node-level
// predicates (role, cell, host, target build, deploy state).
package filter

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/hashjoin/hammerdown/fleet"
)

// All matches every shard of a keyspace.
const All = "ALL"

// ErrInvalid is returned for unparsable filters.
var ErrInvalid = errors.New("invalid filter")

// Spec maps keyspace patterns to shard patterns.
type Spec map[string][]string

// ParseSpec parses "keyspace/shard[,shard...][|keyspace/shard...]".
func ParseSpec(s string) (Spec, error) {
	spec := Spec{}
	s = strings.TrimSpace(s)
	if s == "" {
		return spec, nil
	}

	for _, entry := range strings.Split(s, "|") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		keyspace, shards, found := strings.Cut(entry, "/")
		keyspace = strings.TrimSpace(keyspace)
		if keyspace == "" {
			return nil, fmt.Errorf("%w: empty keyspace in %q", ErrInvalid, entry)
		}
		if _, err := path.Match(keyspace, ""); err != nil {
			return nil, fmt.Errorf("%w: bad keyspace pattern %q: %w", ErrInvalid, keyspace, err)
		}

		if !found || strings.TrimSpace(shards) == "" {
			spec[keyspace] = append(spec[keyspace], All)
			continue
		}

		for _, shard := range strings.Split(shards, ",") {
			shard = strings.TrimSpace(shard)
			if shard == "" {
				return nil, fmt.Errorf("%w: empty shard in %q", ErrInvalid, entry)
			}
			if _, err := path.Match(shard, ""); err != nil {
				return nil, fmt.Errorf("%w: bad shard pattern %q: %w", ErrInvalid, shard, err)
			}
			spec[keyspace] = append(spec[keyspace], shard)
		}
	}

	return spec, nil
}

// Empty reports whether the spec has no entries.
func (s Spec) Empty() bool { return len(s) == 0 }

// Match reports whether keyspace/shard is selected by the spec.
func (s Spec) Match(keyspace, shard string) bool {
	for keyspacePattern, shardPatterns := range s {
		if ok, _ := path.Match(keyspacePattern, keyspace); !ok {
			continue
		}
		for _, p := range shardPatterns {
			if p == All || p == "*" {
				return true
			}
			if ok, _ := path.Match(p, shard); ok {
				return true
			}
		}
	}

	return false
}

// String renders the spec back in canonical (sorted) form.
func (s Spec) String() string {
	keyspaces := make([]string, 0, len(s))
	for c := range s {
		keyspaces = append(keyspaces, c)
	}
	sort.Strings(keyspaces)

	parts := make([]string, 0, len(keyspaces))
	for _, c := range keyspaces {
		parts = append(parts, c+"/"+strings.Join(s[c], ","))
	}

	return strings.Join(parts, "|")
}

// Filter selects nodes.
type Filter struct {
	Limit       Spec     // only these keyspace/shards (empty = everything)
	Skip        Spec     // never these keyspace/shards (wins over Limit)
	Hosts       []string // host globs (empty = any)
	Cells       []string // cells (empty = any)
	PrimaryOnly bool     // only primaries
	ReplicaOnly bool     // only non-primaries
	Target      string   // only nodes whose target build is this
	ForceOK     bool     // also pick nodes in OK state (forced restart)

	// ShardsPerKeyspace caps how many shards of each keyspace are planned.
	// 1 is the classic canary: "one shard of everything".
	ShardsPerKeyspace int
}

// Validate checks for contradictory options.
func (f *Filter) Validate() error {
	if f.PrimaryOnly && f.ReplicaOnly {
		return fmt.Errorf("%w: --primary-only and --replica-only are mutually exclusive", ErrInvalid)
	}
	if f.ShardsPerKeyspace < 0 {
		return fmt.Errorf("%w: --shards-per-keyspace must be >= 0", ErrInvalid)
	}
	for _, h := range f.Hosts {
		if _, err := path.Match(h, ""); err != nil {
			return fmt.Errorf("%w: bad host pattern %q: %w", ErrInvalid, h, err)
		}
	}

	return nil
}

// InScope applies only the "where" predicates (limit, skip, cell, host).
// Reports use it so you can look at a slice of the fleet regardless of state.
func (f *Filter) InScope(n *fleet.Node) (bool, string) {
	if f.Skip.Match(n.Keyspace, n.Shard) {
		return false, "skipped by --skip"
	}
	if !f.Limit.Empty() && !f.Limit.Match(n.Keyspace, n.Shard) {
		return false, "outside --limit"
	}
	if len(f.Cells) > 0 && !contains(f.Cells, n.Cell) {
		return false, "outside --cell"
	}
	if len(f.Hosts) > 0 && !matchAny(f.Hosts, n.Host) {
		return false, "outside --host"
	}

	return true, ""
}

// Match applies every predicate: is this node a restart target?
// The second return value explains a rejection (handy with --debug).
func (f *Filter) Match(n *fleet.Node) (bool, string) {
	if ok, why := f.InScope(n); !ok {
		return false, why
	}

	switch n.State {
	case fleet.StateRestartWait:
	case fleet.StateOK:
		if !f.ForceOK {
			return false, "state OK (use --force-ok)"
		}
	default:
		return false, fmt.Sprintf("state %s", n.State)
	}

	if f.Target != "" && n.Target != f.Target {
		return false, fmt.Sprintf("target %s != --target %s", n.Target, f.Target)
	}
	if f.PrimaryOnly && !n.IsPrimary() {
		return false, "not a primary"
	}
	if f.ReplicaOnly && n.IsPrimary() {
		return false, "is a primary"
	}

	return true, ""
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}

	return false
}

func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, s); ok {
			return true
		}
	}

	return false
}

// SplitList splits a comma separated flag value, dropping blanks.
func SplitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}

	return out
}
