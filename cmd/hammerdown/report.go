package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/report"
)

func cmdReport(args []string) error {
	c := newCommon("report", "Print fleet state. --limit/--skip/--host/--cell scope the report.\n")
	asJSON := c.fs.Bool("json", false, "emit JSON (for scripts and Claude)")
	if err := c.parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	be, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer be.Close(ctx)

	nodes, err := be.Inventory().Nodes(ctx)
	if err != nil {
		return err
	}

	var scoped []*fleet.Node
	for _, n := range nodes {
		if ok, _ := c.flt.InScope(n); ok {
			scoped = append(scoped, n)
		}
	}

	s := report.Summarize(scoped)
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")

		return enc.Encode(s)
	}
	s.WriteText(os.Stdout)

	return nil
}
