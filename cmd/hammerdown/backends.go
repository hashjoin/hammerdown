package main

import (
	"github.com/hashjoin/hammerdown/backend"
	"github.com/hashjoin/hammerdown/backend/shell"
	"github.com/hashjoin/hammerdown/backend/sim"
)

// backends is THE injection point. To teach hammerdown about your
// infrastructure, write a package that implements backend.Factory and add
// it here. That's it - flags, help and --backend selection follow.
//
//	return []backend.Factory{
//		sim.NewFactory(),
//		shell.NewFactory(),
//		vitess.NewFactory(), // <- yours
//	}
func backends() []backend.Factory {
	return []backend.Factory{
		sim.NewFactory(),
		shell.NewFactory(),
	}
}
