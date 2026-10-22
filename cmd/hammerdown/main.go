// Command hammerdown is a CLI control plane for rolling restarts and
// upgrades across large sharded fleets. Put the hammer down. Keep the brakes.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
)

// set with -ldflags "-X main.version=..."
var version = "dev"

// logFlags: UTC with microseconds, so log lines line up with dashboards and
// other hosts' logs regardless of where the operator sits.
const logFlags = log.LstdFlags | log.Lmicroseconds | log.LUTC

const banner = `hammerdown - put the hammer down, keep the brakes.

A CLI control plane for rolling restarts and upgrades across large
sharded fleets: thousands of nodes, explicit workers, exact fail-fast.

Usage:
  hammerdown <command> [flags]

Commands:
  report     fleet state: counts by state, Keyspace:Build table, build matrix
  roll       restart RestartWait nodes shard by shard (dry-run by default)
  sim        manage the simulated 11,000-node demo fleet (init, stage)
  backends   list compiled-in backends
  version    print version

Run 'hammerdown <command> -h' for flags.

Quick start (simulated fleet, nothing real is touched):
  hammerdown sim init
  hammerdown sim stage --to v20.0.1
  hammerdown report
  hammerdown roll --workers 64                             # dry run
  hammerdown roll --workers 64 --dry-run=false --yes       # for real (sim)
`

// exit codes
const (
	exitOK       = 0
	exitFailures = 1
	exitUsage    = 2
)

func main() {
	log.SetFlags(logFlags)
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, banner)

		return exitUsage
	}

	var err error
	switch args[0] {
	case "report":
		err = cmdReport(args[1:])
	case "roll":
		err = cmdRoll(args[1:])
	case "sim":
		err = cmdSim(args[1:])
	case "backends":
		for _, f := range backends() {
			fmt.Printf("%-10s %s\n", f.Name(), f.Describe())
		}
	case "version", "--version", "-v":
		fmt.Println("hammerdown", version)
	case "help", "-h", "--help":
		fmt.Print(banner)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", args[0], banner)

		return exitUsage
	}

	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case errors.Is(err, errUsage):
		fmt.Fprintln(os.Stderr, "error:", err)

		return exitUsage
	default:
		log.Printf("ERROR: %v", err)

		return exitFailures
	}
}

var errUsage = errors.New("usage")
