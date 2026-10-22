package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/hashjoin/hammerdown/backend"
	"github.com/hashjoin/hammerdown/filter"
)

// common holds flags shared by every fleet command.
type common struct {
	fs        *flag.FlagSet
	backend   string
	factories map[string]backend.Factory
	debug     bool

	limit, skip, hosts, cells string
	flt                       filter.Filter
}

func newCommon(name, usage string) *common {
	c := &common{fs: flag.NewFlagSet(name, flag.ContinueOnError), factories: map[string]backend.Factory{}}

	var names []string
	for _, f := range backends() {
		c.factories[f.Name()] = f
		names = append(names, f.Name())
		f.RegisterFlags(c.fs)
	}
	sort.Strings(names)

	c.fs.StringVar(&c.backend, "backend", "sim", "backend: "+strings.Join(names, ", "))
	c.fs.BoolVar(&c.debug, "debug", false, "verbose output (per-node plan, skip reasons)")

	// where
	c.fs.StringVar(&c.limit, "limit", "", "only these keyspace/shards, e.g. 'users/ALL|teams/-80,80-|mess*/4*'")
	c.fs.StringVar(&c.skip, "skip", "", "never these keyspace/shards (same syntax; wins over --limit)")
	c.fs.StringVar(&c.hosts, "host", "", "only hosts matching these globs (comma separated)")
	c.fs.StringVar(&c.cells, "cell", "", "only these cells / AZs (comma separated)")

	c.fs.Usage = func() {
		fmt.Fprintf(c.fs.Output(), "Usage: hammerdown %s [flags]\n\n%s\nFlags:\n", name, usage)
		c.fs.PrintDefaults()
	}

	return c
}

// targetFlags adds the "which nodes are restart targets" flags.
func (c *common) targetFlags() {
	c.fs.BoolVar(&c.flt.PrimaryOnly, "primary-only", false, "only primaries")
	c.fs.BoolVar(&c.flt.ReplicaOnly, "replica-only", false, "only replicas / rdonly")
	c.fs.StringVar(&c.flt.Target, "target", "", "only nodes whose target build is this")
	c.fs.BoolVar(&c.flt.ForceOK, "force-ok", false, "also restart nodes already in OK state")
	c.fs.IntVar(&c.flt.ShardsPerKeyspace, "shards-per-keyspace", 0, "plan at most N shards per keyspace (1 = canary)")
}

func (c *common) parse(args []string) error {
	if err := c.fs.Parse(args); err != nil {
		return err
	}
	if c.fs.NArg() > 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, c.fs.Args())
	}

	var err error
	if c.flt.Limit, err = filter.ParseSpec(c.limit); err != nil {
		return fmt.Errorf("%w: --limit: %w", errUsage, err)
	}
	if c.flt.Skip, err = filter.ParseSpec(c.skip); err != nil {
		return fmt.Errorf("%w: --skip: %w", errUsage, err)
	}
	c.flt.Hosts = filter.SplitList(c.hosts)
	c.flt.Cells = filter.SplitList(c.cells)

	if err := c.flt.Validate(); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if _, ok := c.factories[c.backend]; !ok {
		return fmt.Errorf("%w: unknown --backend %q", errUsage, c.backend)
	}

	return nil
}

func (c *common) open(ctx context.Context) (backend.Backend, error) {
	logger := log.New(os.Stderr, "", logFlags)

	return c.factories[c.backend].Open(ctx, logger)
}
