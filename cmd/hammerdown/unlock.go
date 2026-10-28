package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashjoin/hammerdown/lock"
)

const unlockUsage = `Show who holds a hammerdown lock and, only if its holder is provably gone,
release it. There is no --force.

Without --reason this only inspects. With --reason it releases the lock when:

  - the holder is on this host and its pid is not running;
  - the holder is on another host, its heartbeat is older than --stale-after,
    and --confirm-host names that host (you checked there first).

It refuses when the holder's pid is running on this host: that roll is alive.
Reattach to it (tmux/screen), stop it with Ctrl-C (it drains and releases the
lock), or wait for it to finish. A dropped SSH session is not a dead roll.

Every release is appended to unlock.log next to the lock files.

Examples:
  hammerdown unlock --backend shell --shell-inventory ./inv.sh
  hammerdown unlock --backend shell --shell-inventory ./inv.sh --reason "jump1 rebooted"
  hammerdown unlock ... --confirm-host jump-eu-1 --reason "checked: no hammerdown on jump-eu-1"
`

var errUnlockRefused = errors.New("refusing to release the lock")

func cmdUnlock(args []string) error {
	c := newCommon("unlock", unlockUsage)

	var (
		key, reason, confirmHost string
		staleAfter               time.Duration
	)
	c.fs.StringVar(&key, "key", "process/roll", "lock key, e.g. process/roll or shard/users/-80")
	c.fs.StringVar(&reason, "reason", "", "why the holder is gone; required to release (recorded)")
	c.fs.StringVar(&confirmHost, "confirm-host", "", "holder on another host: type its hostname to confirm you checked there")
	c.fs.DurationVar(&staleAfter, "stale-after", 2*time.Minute, "holder on another host: refuse while its heartbeat is younger than this")

	if err := c.parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	be, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := be.Close(context.Background()); err != nil {
			log.Printf("WARNING: closing backend: %v", err)
		}
	}()

	br, ok := be.Locker().(lock.Breaker)
	if !ok {
		return fmt.Errorf("the %s backend's locks can't be inspected or released from here; "+
			"locks that expire with their holder (leases, sessions, in-memory) don't need it - use your lock service's own tooling", c.backend)
	}

	h, err := br.Holder(ctx, key)
	if err != nil {
		return fmt.Errorf("reading %s: %w", key, err)
	}
	if h == nil {
		fmt.Printf("%s is not held\n", key)

		return nil
	}
	fmt.Printf("%s is held by %s\n", key, h)

	if err := mayBreak(h, reason, confirmHost, staleAfter, time.Now()); err != nil {
		return err
	}
	if reason == "" {
		fmt.Println("inspect only: pass --reason to release it")

		return nil
	}

	if err := br.Break(ctx, h, whoami(), reason); err != nil {
		return err
	}
	fmt.Printf("released %s (recorded in unlock.log)\n", key)

	return nil
}

// mayBreak decides whether h may be released. With no reason it only
// explains what releasing would take.
func mayBreak(h *lock.Holder, reason, confirmHost string, staleAfter time.Duration, now time.Time) error {
	switch h.Liveness {
	case lock.Alive:
		return fmt.Errorf("%w: pid %d is running on this host. The roll is alive even if your SSH session is not: "+
			"reattach to it (tmux/screen), stop it with Ctrl-C there, or wait. 'ps -p %d' shows it",
			errUnlockRefused, h.PID, h.PID)
	case lock.Dead:
		return nil
	}

	if age := now.Sub(h.Heartbeat); age < staleAfter {
		return fmt.Errorf("%w: holder on %s heartbeat %s ago (< --stale-after %s): it looks alive",
			errUnlockRefused, h.Host, age.Round(time.Second), staleAfter)
	}
	if reason != "" && !strings.EqualFold(confirmHost, h.Host) {
		return fmt.Errorf("%w: holder is on %s; check there that no hammerdown is running, then pass --confirm-host %s",
			errUnlockRefused, h.Host, h.Host)
	}

	return nil
}

// explainHeld turns "process lock is held" into what to do about it.
func explainHeld(err error) {
	var he *lock.HeldError
	if !errors.As(err, &he) {
		return
	}

	h := he.Holder
	switch h.Liveness {
	case lock.Alive:
		log.Printf("the holder (pid %d on this host) is RUNNING. Do not remove the lock: reattach to it (tmux/screen), "+
			"stop it with Ctrl-C there, or wait. 'ps -p %d' shows it", h.PID, h.PID)
	case lock.Dead:
		log.Printf("pid %d is not running on this host: the lock is stale. Release it with: "+
			"hammerdown unlock <same --backend flags> --reason '<what happened>'", h.PID)
	default:
		log.Printf("the holder is on %s. Check there first; if nothing is running: "+
			"hammerdown unlock <same --backend flags> --confirm-host %s --reason '<what you checked>'", h.Host, h.Host)
	}
}
