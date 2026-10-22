package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hashjoin/hammerdown/report"
	"github.com/hashjoin/hammerdown/roll"
)

const rollUsage = `Restart every node in RestartWait (or OK with --force-ok) that passes the
filters, one shard per worker: replicas first, then a planned failover, then
the demoted primary. Dry run unless --dry-run=false.

Ctrl-C once: drain (in-flight node operations finish, nothing new starts).
Ctrl-C twice: abort in-flight operations. Three times: exit immediately.

Examples:
  hammerdown roll --limit 'users/ALL|teams/ALL' --workers 32
  hammerdown roll --shards-per-keyspace 1 --dry-run=false --yes        # canary
  hammerdown roll --skip 'billing/ALL' --replica-only --workers 128
  hammerdown roll --stop-at 2h --blackout 06:00-11:00 --blackout-tz America/Los_Angeles
`

var errInterrupted = errors.New("interrupted")

func cmdRoll(args []string) error {
	c := newCommon("roll", rollUsage)
	c.targetFlags()

	var (
		b       roll.Batch
		stopAt  string
		window  string
		tz      string
		yes     bool
		quiet   bool
		jitterF time.Duration
	)
	b.DryRun = true
	c.fs.IntVar(&b.Workers, "workers", 8, "max shards in flight across the fleet")
	c.fs.IntVar(&b.KeyspaceWorkers, "keyspace-workers", 0, "max shards in flight per keyspace (0 = no cap)")
	c.fs.BoolVar(&b.DryRun, "dry-run", true, "log what would happen, touch nothing")
	c.fs.IntVar(&b.MaxPrimaryFailures, "max-primary-failures", 1, "drain after this many primary-side failures (0 = never)")
	c.fs.IntVar(&b.MaxReplicaFailures, "max-replica-failures", 5, "drain after this many replica-side failures (0 = never)")
	c.fs.StringVar(&stopAt, "stop-at", "", "start no new shards after this time: RFC3339 or a duration from now (e.g. 90m)")
	c.fs.StringVar(&window, "blackout", "", "daily window with no new shards, e.g. 06:00-11:00")
	c.fs.StringVar(&tz, "blackout-tz", "", "IANA time zone for --blackout (default: local)")
	c.fs.DurationVar(&b.LockValidateInterval, "lock-validate", 30*time.Second, "process lock re-validation interval")
	c.fs.DurationVar(&jitterF, "jitter", -1, "override the backend's jitter before each restart (e.g. 2s)")
	c.fs.BoolVar(&yes, "yes", false, "don't prompt before a real (non dry-run) roll")
	c.fs.BoolVar(&quiet, "quiet", false, "skip the pre-roll fleet report")

	if err := c.parse(args); err != nil {
		return err
	}
	if b.Workers < 1 {
		return fmt.Errorf("%w: --workers must be >= 1", errUsage)
	}

	if stopAt != "" {
		t, err := parseStopAt(stopAt, time.Now())
		if err != nil {
			return fmt.Errorf("%w: --stop-at: %w", errUsage, err)
		}
		b.StopAt = t
	}
	if window != "" {
		w, err := roll.ParseWindow(window, tz)
		if err != nil {
			return fmt.Errorf("%w: --blackout: %w", errUsage, err)
		}
		b.Blackout = w
	}

	// Two contexts: `hard` dies on the second Ctrl-C and aborts in-flight
	// node operations; the batch's own drain handles the first one.
	hard, abort := context.WithCancelCause(context.Background())
	defer abort(nil)

	be, err := c.open(hard)
	if err != nil {
		return err
	}
	defer func() {
		if err := be.Close(context.Background()); err != nil {
			log.Printf("WARNING: closing backend: %v", err)
		}
	}()

	nodes, err := be.Inventory().Nodes(hard)
	if err != nil {
		return err
	}

	if !quiet {
		report.Summarize(nodes).WriteText(os.Stdout)
	}

	if c.debug {
		for _, n := range nodes {
			if ok, why := c.flt.Match(n); !ok && why != "state OK (use --force-ok)" {
				log.Printf("debug: not a target: %-45s %s", n.String(), why)
			}
		}
	}

	shards := roll.Plan(nodes, &c.flt)
	report.WritePlan(os.Stdout, shards, b.Workers, b.KeyspaceWorkers, b.DryRun, c.debug)
	if len(shards) == 0 {
		fmt.Println("nothing to do: no node matched (is anything in RestartWait? try 'report')")

		return nil
	}

	if !b.DryRun && !yes {
		if err := confirm(shards); err != nil {
			return err
		}
	}

	b.Processor = be.Processor()
	b.Locker = be.Locker()
	b.Guards = be.Guards()
	b.Log = log.New(os.Stderr, "", logFlags)
	b.LockReason = "hammerdown roll by " + whoami()
	if jitterF >= 0 {
		if la, ok := b.Processor.(*roll.LeaderAware); ok {
			la.Jitter = jitterF
		}
	}

	go handleSignals(&b, abort)

	fmt.Printf("--------- Rolling ---------\n")
	if !b.StopAt.IsZero() {
		fmt.Printf("> no new shards after %s\n", b.StopAt.Format(time.RFC3339))
	}
	if b.Blackout != nil {
		fmt.Printf("> no new shards inside blackout %s\n", b.Blackout)
	}

	res, runErr := b.Run(hard, shards)
	if res != nil {
		report.WriteResult(os.Stdout, res)
	}

	return runErr
}

func handleSignals(b *roll.Batch, abort context.CancelCauseFunc) {
	sigs := make(chan os.Signal, 3)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	for i := 1; ; i++ {
		s := <-sigs
		switch i {
		case 1:
			log.Printf("received %s: draining - in-flight operations finish, nothing new starts (again to abort)", s)
			b.Drain(fmt.Errorf("%w: received %s", errInterrupted, s))
		case 2:
			log.Printf("received %s again: aborting in-flight operations (again to exit immediately)", s)
			abort(fmt.Errorf("%w: received %s twice", errInterrupted, s))
		default:
			log.Printf("received %s a third time: exiting NOW - check for leftover locks", s)
			os.Exit(130)
		}
	}
}

func parseStopAt(s string, now time.Time) (time.Time, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return now.Add(d), nil
	}

	return time.Parse(time.RFC3339, s)
}

func confirm(shards []*roll.Shard) error {
	st := roll.Stats(shards)
	if fi, err := os.Stdin.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
		return fmt.Errorf("%w: refusing a real roll without a terminal; pass --yes", errUsage)
	}

	fmt.Printf("About to restart %d node(s) in %d shard(s) across %d keyspace(s). Type 'hammerdown' to continue: ",
		st.Nodes, st.Shards, st.Keyspaces)

	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.TrimSpace(line) != "hammerdown" {
		return errors.New("aborted by operator")
	}

	return nil
}

func whoami() string {
	host, _ := os.Hostname()
	user := os.Getenv("USER")
	if user == "" {
		user = "unknown"
	}

	return user + "@" + host
}
