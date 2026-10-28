package lock

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func exercise(t *testing.T, l Locker) {
	t.Helper()
	ctx := context.Background()

	a, err := l.Lock(ctx, "shard/users/-80", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Lock(ctx, "shard/users/-80", "test"); !errors.Is(err, ErrHeld) {
		t.Fatalf("expected ErrHeld, got %v", err)
	}
	if err := a.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := l.Lock(ctx, "shard/users/-80", "test")
	if err != nil {
		t.Fatalf("relock after unlock: %v", err)
	}
	_ = b.Unlock(ctx)
}

func TestMemory(t *testing.T) { exercise(t, NewMemory()) }

func TestDir(t *testing.T) { exercise(t, &Dir{Path: t.TempDir()}) }

func TestMemoryLost(t *testing.T) {
	m := NewMemory()
	l, _ := m.Lock(context.Background(), "process/roll", "")
	m.Steal("process/roll")
	if err := l.Validate(context.Background()); !errors.Is(err, ErrLost) {
		t.Fatalf("expected ErrLost, got %v", err)
	}
}

// deadPID returns the pid of a process that has already exited.
func deadPID(t *testing.T) int {
	t.Helper()
	bin, err := exec.LookPath("true")
	if err != nil {
		bin = os.Args[0] // re-exec this test binary; slower
	}
	cmd := exec.Command(bin, "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	return cmd.Process.Pid
}

// rewrite edits the record of a held Dir lock, as another process would
// have written it.
func rewrite(t *testing.T, d *Dir, key string, edit func(*dirRecord)) {
	t.Helper()
	b, err := os.ReadFile(d.file(key))
	if err != nil {
		t.Fatal(err)
	}
	var rec dirRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	edit(&rec)
	b, _ = json.Marshal(rec)
	if err := os.WriteFile(d.file(key), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The 4 a.m. moment: the lock is held by a session the operator
// believes is dead. The error must say it is running, and never suggest
// deleting the lock.
func TestDirHeldByRunningHolder(t *testing.T) {
	d := &Dir{Path: t.TempDir()}
	ctx := context.Background()
	if _, err := d.Lock(ctx, "process/roll", "first"); err != nil {
		t.Fatal(err)
	}

	_, err := d.Lock(ctx, "process/roll", "second")
	var he *HeldError
	if !errors.As(err, &he) || !errors.Is(err, ErrHeld) {
		t.Fatalf("want HeldError wrapping ErrHeld, got %v", err)
	}
	if he.Holder.Liveness != Alive || he.Holder.PID != os.Getpid() {
		t.Errorf("holder %+v: want this (running) process", he.Holder)
	}
	if strings.Contains(strings.ToLower(err.Error()), "remove") {
		t.Errorf("error must not suggest removing the lock: %v", err)
	}
	if err := d.Break(ctx, he.Holder, "tester", "I think it's dead"); err == nil {
		t.Fatal("Break released a lock whose holder is running")
	}
	if h, _ := d.Holder(ctx, "process/roll"); h == nil {
		t.Fatal("lock is gone after a refused Break")
	}
}

func TestDirBreakDeadHolder(t *testing.T) {
	d := &Dir{Path: t.TempDir()}
	ctx := context.Background()
	if _, err := d.Lock(ctx, "process/roll", "roll"); err != nil {
		t.Fatal(err)
	}
	pid := deadPID(t)
	rewrite(t, d, "process/roll", func(r *dirRecord) { r.PID = pid })

	h, err := d.Holder(ctx, "process/roll")
	if err != nil || h.Liveness != Dead {
		t.Fatalf("want a dead holder, got %+v %v", h, err)
	}
	if err := d.Break(ctx, h, "tester", ""); err == nil {
		t.Error("Break without a reason must fail")
	}
	if err := d.Break(ctx, h, "tester", "jump box rebooted"); err != nil {
		t.Fatal(err)
	}

	log, err := os.ReadFile(filepath.Join(d.Path, "unlock.log"))
	if err != nil || !strings.Contains(string(log), "jump box rebooted") || !strings.Contains(string(log), `"by":"tester"`) {
		t.Errorf("unlock.log: %s %v", log, err)
	}
	l, err := d.Lock(ctx, "process/roll", "after")
	if err != nil {
		t.Fatalf("relock after break: %v", err)
	}
	_ = l.Unlock(ctx)
}

func TestDirBreakRefusesChangedHands(t *testing.T) {
	d := &Dir{Path: t.TempDir()}
	ctx := context.Background()
	_, _ = d.Lock(ctx, "process/roll", "old")
	pid := deadPID(t)
	rewrite(t, d, "process/roll", func(r *dirRecord) { r.PID = pid })
	inspected, _ := d.Holder(ctx, "process/roll")

	_ = os.Remove(d.file("process/roll")) // someone else cleaned up, and a new roll took the lock
	if _, err := d.Lock(ctx, "process/roll", "new"); err != nil {
		t.Fatal(err)
	}
	if err := d.Break(ctx, inspected, "tester", "stale"); err == nil {
		t.Fatal("Break released a lock that changed hands since it was inspected")
	}
}

func TestDirOtherHostIsUnknown(t *testing.T) {
	d := &Dir{Path: t.TempDir()}
	ctx := context.Background()
	_, _ = d.Lock(ctx, "process/roll", "roll")
	rewrite(t, d, "process/roll", func(r *dirRecord) { r.Host = "jump-eu-1.example" })

	h, _ := d.Holder(ctx, "process/roll")
	if h.Liveness != Unknown || h.Host != "jump-eu-1.example" {
		t.Errorf("want unknown liveness on another host, got %+v", h)
	}
}

func TestDirValidateHeartbeats(t *testing.T) {
	d := &Dir{Path: t.TempDir()}
	ctx := context.Background()
	l, _ := d.Lock(ctx, "process/roll", "roll")
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(d.file("process/roll"), old, old)

	if err := l.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	h, _ := d.Holder(ctx, "process/roll")
	if time.Since(h.Heartbeat) > time.Minute {
		t.Errorf("heartbeat not refreshed: %s", h.Heartbeat)
	}
}
