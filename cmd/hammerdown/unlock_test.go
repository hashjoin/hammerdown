package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashjoin/hammerdown/lock"
)

func TestMayBreak(t *testing.T) {
	now := time.Now()
	old := now.Add(-time.Hour)
	cases := []struct {
		name        string
		h           lock.Holder
		reason, cfm string
		ok          bool
	}{
		{"alive is never released", lock.Holder{Liveness: lock.Alive, Heartbeat: old}, "it's dead", "", false},
		{"alive, inspect only", lock.Holder{Liveness: lock.Alive}, "", "", false},
		{"dead on this host", lock.Holder{Liveness: lock.Dead, Heartbeat: now}, "rebooted", "", true},
		{"other host, fresh heartbeat", lock.Holder{Liveness: lock.Unknown, Host: "eu1", Heartbeat: now}, "x", "eu1", false},
		{"other host, stale, unconfirmed", lock.Holder{Liveness: lock.Unknown, Host: "eu1", Heartbeat: old}, "x", "", false},
		{"other host, stale, wrong host", lock.Holder{Liveness: lock.Unknown, Host: "eu1", Heartbeat: old}, "x", "eu2", false},
		{"other host, stale, confirmed", lock.Holder{Liveness: lock.Unknown, Host: "eu1", Heartbeat: old}, "x", "EU1", true},
		{"other host, stale, inspect only", lock.Holder{Liveness: lock.Unknown, Host: "eu1", Heartbeat: old}, "", "", true},
	}
	for _, c := range cases {
		err := mayBreak(&c.h, c.reason, c.cfm, 2*time.Minute, now)
		if (err == nil) != c.ok {
			t.Errorf("%s: err=%v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, errUnlockRefused) {
			t.Errorf("%s: want errUnlockRefused, got %v", c.name, err)
		}
	}
}

// End to end: a lock left by a dead roll is released only with a reason;
// a lock held by a running process is never released.
func TestCLIUnlock(t *testing.T) {
	dir := t.TempDir()
	d := &lock.Dir{Path: dir}
	devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	stdout := os.Stdout
	os.Stdout = devnull
	defer func() { os.Stdout = stdout }()

	ctx := context.Background()
	if _, err := d.Lock(ctx, "process/roll", "roll"); err != nil {
		t.Fatal(err)
	}
	unlock := func(extra ...string) int {
		return run(append([]string{"unlock", "--backend", "shell", "--shell-inventory", "true", "--shell-lock-dir", dir}, extra...))
	}

	// held by this (running) test process
	if code := unlock("--reason", "pretty sure it died"); code != exitFailures {
		t.Fatalf("released a running holder's lock: exit %d", code)
	}

	// now make the holder a dead pid
	bin, err := exec.LookPath("true")
	if err != nil {
		t.Skip("no 'true' binary")
	}
	cmd := exec.Command(bin)
	_ = cmd.Run()
	file := filepath.Join(dir, "process__roll.lock")
	b, _ := os.ReadFile(file)
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	rec["pid"] = cmd.Process.Pid
	b, _ = json.Marshal(rec)
	_ = os.WriteFile(file, b, 0o600)

	if code := unlock(); code != exitOK {
		t.Fatalf("inspect exited %d", code)
	}
	if h, _ := d.Holder(ctx, "process/roll"); h == nil {
		t.Fatal("inspect-only released the lock")
	}
	if code := unlock("--reason", "roll process gone after reboot"); code != exitOK {
		t.Fatalf("release exited %d", code)
	}
	if h, _ := d.Holder(ctx, "process/roll"); h != nil {
		t.Fatal("lock still held after release")
	}

	// sim: in-memory locks, nothing to unlock
	if code := run([]string{"unlock", "--sim-state", filepath.Join(dir, "fleet.json")}); code != exitFailures {
		t.Errorf("sim unlock exited %d", code)
	}
}
