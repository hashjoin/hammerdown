// Package backend is the seam between the hammerdown engine and your
// infrastructure. A Backend bundles the four things the engine needs:
//
//	Inventory  - what is out there (fleet.Inventory)
//	Processor  - how to roll one shard (roll.Processor, usually
//	             &roll.LeaderAware{Ops: yourNodeOps})
//	Locker     - process + shard locks your other automation respects
//	Guards     - pre-flight checks per shard (optional)
//
// Backends are wired into the binary explicitly in cmd/hammerdown/backends.go.
// No init() magic, no plugins: add a package, add one line, rebuild.
package backend

import (
	"context"
	"flag"

	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
	"github.com/hashjoin/hammerdown/roll"
)

// Backend is an opened connection to a fleet.
type Backend interface {
	Inventory() fleet.Inventory
	Processor() roll.Processor
	Locker() lock.Locker
	Guards() []roll.Guard
	// Close flushes and releases resources. Called once, even after errors.
	Close(ctx context.Context) error
}

// Factory creates a Backend. Each factory owns its flags; prefix them with
// the backend name (--sim-*, --shell-*, --vitess-*) to keep -h readable.
type Factory interface {
	Name() string
	Describe() string
	RegisterFlags(fs *flag.FlagSet)
	Open(ctx context.Context, log roll.Logger) (Backend, error)
}
