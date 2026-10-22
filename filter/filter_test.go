package filter

import (
	"testing"

	"github.com/hashjoin/hammerdown/fleet"
)

func TestParseSpec(t *testing.T) {
	spec, err := ParseSpec("users/ALL | teams/-80,80- |mess*/4* | audit")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		keyspace, shard string
		want            bool
	}{
		{"users", "-40", true},
		{"teams", "-80", true},
		{"teams", "80-", true},
		{"teams", "40-80", false},
		{"messages", "4000-4080", true},
		{"messages", "5000-5080", false},
		{"audit", "0", true},
		{"files", "0", false},
	}
	for _, c := range cases {
		if got := spec.Match(c.keyspace, c.shard); got != c.want {
			t.Errorf("Match(%s/%s) = %v, want %v", c.keyspace, c.shard, got, c.want)
		}
	}
}

func TestParseSpecErrors(t *testing.T) {
	for _, bad := range []string{"/ALL", "users/a,,b", "[/ALL", "users/["} {
		if _, err := ParseSpec(bad); err == nil {
			t.Errorf("ParseSpec(%q) expected error", bad)
		}
	}
}

func node(keyspace, shard string, role fleet.Role, state fleet.State) *fleet.Node {
	return &fleet.Node{Keyspace: keyspace, Shard: shard, Role: role, State: state, Host: "h-" + shard, Cell: "az1", Target: "v2"}
}

func TestFilterMatch(t *testing.T) {
	limit, _ := ParseSpec("users/ALL|teams/ALL")
	skip, _ := ParseSpec("teams/-80")
	f := &Filter{Limit: limit, Skip: skip}

	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateRestartWait)); !ok {
		t.Error("users/-80 RestartWait should match")
	}
	if ok, _ := f.Match(node("teams", "-80", fleet.Replica, fleet.StateRestartWait)); ok {
		t.Error("skip must win over limit")
	}
	if ok, _ := f.Match(node("files", "-80", fleet.Replica, fleet.StateRestartWait)); ok {
		t.Error("files outside limit")
	}
	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateOK)); ok {
		t.Error("OK state needs --force-ok")
	}
	f.ForceOK = true
	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateOK)); !ok {
		t.Error("OK state with --force-ok should match")
	}
	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateDrained)); ok {
		t.Error("drained never matches")
	}

	f.PrimaryOnly = true
	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateRestartWait)); ok {
		t.Error("primary-only rejects replicas")
	}

	f = &Filter{Target: "v3"}
	if ok, why := f.Match(node("users", "-80", fleet.Replica, fleet.StateRestartWait)); ok || why == "" {
		t.Error("target mismatch should be rejected with a reason")
	}

	f = &Filter{Hosts: []string{"h-8*"}, Cells: []string{"az1"}}
	if ok, _ := f.Match(node("users", "80-", fleet.Replica, fleet.StateRestartWait)); !ok {
		t.Error("host glob should match")
	}
	if ok, _ := f.Match(node("users", "-80", fleet.Replica, fleet.StateRestartWait)); ok {
		t.Error("host glob should not match")
	}
}

func TestValidate(t *testing.T) {
	if err := (&Filter{PrimaryOnly: true, ReplicaOnly: true}).Validate(); err == nil {
		t.Error("expected error")
	}
}
