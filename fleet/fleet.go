// Package fleet defines the node model every hammerdown component speaks.
//
// A fleet is a flat list of nodes. Each node belongs to a keyspace and a
// shard inside that keyspace. The vocabulary is Vitess's; for other systems
// read "keyspace" as the logical database or cluster (a Cassandra/Scylla
// keyspace, a MongoDB or Redis cluster, a Postgres/Patroni scope ...) and
// "shard" as its replica set or partition.
// Exactly one node per shard is expected to be the primary.
//
// Versions are opaque strings. hammerdown only ever compares them for
// equality: a node "needs a restart" when Running != Target.
package fleet

import (
	"context"
	"fmt"
)

// Role is a node's replication role inside its shard.
type Role string

// Roles.
const (
	Primary  Role = "primary"
	Replica  Role = "replica" // eligible to be promoted
	ReadOnly Role = "rdonly"  // restarted like a replica, never promoted
)

// State is a node's deploy state, as reported by whatever agent manages it.
type State string

const (
	// StateOK - node runs what it should; nothing to do.
	StateOK State = "OK"
	// StateRestartWait - a new build is staged and the node is waiting for
	// the control plane to restart it. This is what `roll` picks up.
	StateRestartWait State = "RestartWait"
	// StateDrained - node is out of service; never restarted.
	StateDrained State = "Drained"
)

// Node is one process/host in the fleet.
type Node struct {
	Name     string            `json:"name"`               // unique id (e.g. a Vitess tablet alias)
	Host     string            `json:"host"`               // where to reach it
	Keyspace string            `json:"keyspace"`           // Vitess keyspace / logical database
	Shard    string            `json:"shard"`              // shard / replica set
	Cell     string            `json:"cell,omitempty"`     // AZ / cell / rack
	Role     Role              `json:"role"`               // primary / replica / rdonly
	State    State             `json:"state"`              // OK / RestartWait / Drained
	Deployed string            `json:"deployed,omitempty"` // build installed on disk
	Running  string            `json:"running"`            // build currently running
	Target   string            `json:"target"`             // build it should be running
	Labels   map[string]string `json:"labels,omitempty"`   // anything else your backend wants
}

// IsPrimary reports whether n is its shard's primary.
func (n *Node) IsPrimary() bool { return n.Role == Primary }

// ShardKey is "keyspace/shard".
func (n *Node) ShardKey() string { return n.Keyspace + "/" + n.Shard }

// String is "keyspace/shard:host" - the form used in every log line.
func (n *Node) String() string { return fmt.Sprintf("%s/%s:%s", n.Keyspace, n.Shard, n.Host) }

// Inventory discovers the fleet. It is called once per command invocation;
// implementations should return a consistent snapshot.
type Inventory interface {
	Nodes(ctx context.Context) ([]*Node, error)
}

// ShardInventory is optionally implemented by an Inventory that can read a
// single shard cheaply (one topo or CMDB call instead of the whole fleet).
//
// hammerdown re-reads every shard after taking its shard lock, and again
// before each node operation, so it never acts on a plan another operator,
// another hammerdown or a failover has made stale. Without ShardNodes
// those re-reads list the whole fleet and filter: correct, just slower.
type ShardInventory interface {
	ShardNodes(ctx context.Context, keyspace, shard string) ([]*Node, error)
}

// InventoryFunc adapts a function to the Inventory interface.
type InventoryFunc func(ctx context.Context) ([]*Node, error)

// Nodes implements Inventory.
func (f InventoryFunc) Nodes(ctx context.Context) ([]*Node, error) { return f(ctx) }
