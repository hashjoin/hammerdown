package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/hashjoin/hammerdown/backend/sim"
)

const simUsage = `Manage the simulated demo fleet (11,000 nodes, 2,829 shards, 20 keyspaces).

  hammerdown sim init [--force]                      build a fresh fleet on ` + sim.StartBuild + `
  hammerdown sim stage --to BUILD [--limit ...]      "deploy" BUILD: nodes not running it go RestartWait

Then use 'report' and 'roll' as you would on a real fleet.
`

func cmdSim(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(os.Stderr, simUsage)

		return nil
	}

	switch args[0] {
	case "init":
		return simInit(args[1:])
	case "stage":
		return simStage(args[1:])
	default:
		return fmt.Errorf("%w: unknown sim command %q", errUsage, args[0])
	}
}

func simInit(args []string) error {
	fs := flag.NewFlagSet("sim init", flag.ContinueOnError)
	f := sim.NewFactory()
	f.RegisterFlags(fs)
	force := fs.Bool("force", false, "overwrite an existing fleet")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := f.Config()

	if _, err := os.Stat(cfg.State); err == nil && !*force {
		return fmt.Errorf("%w: %s exists (use --force to rebuild)", errUsage, cfg.State)
	}

	fl := sim.Generate(cfg.Seed)
	if err := fl.Save(cfg.State); err != nil {
		return err
	}

	fmt.Printf("sim: %d nodes in %s (seed=%d), all running %s\n", len(fl.Nodes), cfg.State, cfg.Seed, sim.StartBuild)

	return nil
}

func simStage(args []string) error {
	c := newCommon("sim stage", "Stage a build on (a slice of) the simulated fleet.\n")
	to := c.fs.String("to", "", "build to stage, e.g. v20.0.1 (required)")
	if err := c.parse(args); err != nil {
		return err
	}
	if *to == "" {
		return fmt.Errorf("%w: --to is required", errUsage)
	}

	// reuse the --sim-state the common flagset parsed
	state := c.fs.Lookup("sim-state").Value.String()

	fl, err := sim.Load(state)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: no sim fleet at %s - run 'hammerdown sim init' first", errUsage, state)
	}
	if err != nil {
		return err
	}

	n := fl.Stage(&c.flt, *to)
	if err := fl.Save(state); err != nil {
		return err
	}

	fmt.Printf("sim: staged %s - %d node(s) now RestartWait\n", *to, n)

	return nil
}
