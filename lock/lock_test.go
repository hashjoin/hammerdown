package lock

import (
	"context"
	"errors"
	"testing"
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
