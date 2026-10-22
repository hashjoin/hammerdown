// Package lock provides the two locks hammerdown takes:
//
//   - a process lock ("process/roll") so two operators can't roll the same
//     fleet at once, validated periodically while the batch runs;
//   - a shard lock ("shard/<keyspace>/<shard>") around every shard, so the
//     roll never races a failover, a resharding job or a node replacement
//     that honors the same locks.
//
// Production backends should implement Locker on top of whatever your other
// automation already respects: Consul sessions, etcd leases, the Vitess
// topo server, ZooKeeper, a DynamoDB conditional write ... Memory and Dir are
// here for demos and single-operator setups.
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	// ErrHeld - somebody else holds the lock.
	ErrHeld = errors.New("lock is held")
	// ErrLost - the lock we held is gone or now belongs to someone else.
	ErrLost = errors.New("lock lost")
)

// Locker hands out locks by key.
type Locker interface {
	Lock(ctx context.Context, key, reason string) (Lock, error)
}

// Lock is a held lock.
type Lock interface {
	// Validate returns ErrLost (wrapped) if the lock is no longer ours.
	Validate(ctx context.Context) error
	// Unlock releases the lock. Safe to call once.
	Unlock(ctx context.Context) error
}

func token() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// Memory is an in-process Locker (demos and tests).
type Memory struct {
	mu   sync.Mutex
	held map[string]string
}

// NewMemory returns an empty in-memory locker.
func NewMemory() *Memory { return &Memory{held: map[string]string{}} }

// Lock implements Locker.
func (m *Memory) Lock(_ context.Context, key, _ string) (Lock, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.held[key]; ok {
		return nil, fmt.Errorf("%w: %s", ErrHeld, key)
	}

	t := token()
	m.held[key] = t

	return &memLock{m: m, key: key, token: t}, nil
}

// Steal forcibly takes over key (tests use it to simulate a lost lock).
func (m *Memory) Steal(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.held[key] = "stolen"
}

// Held reports whether key is currently held.
func (m *Memory) Held(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.held[key]

	return ok
}

type memLock struct {
	m          *Memory
	key, token string
}

func (l *memLock) Validate(context.Context) error {
	l.m.mu.Lock()
	defer l.m.mu.Unlock()

	if l.m.held[l.key] != l.token {
		return fmt.Errorf("%w: %s", ErrLost, l.key)
	}

	return nil
}

func (l *memLock) Unlock(ctx context.Context) error {
	if err := l.Validate(ctx); err != nil {
		return err
	}

	l.m.mu.Lock()
	defer l.m.mu.Unlock()
	delete(l.m.held, l.key)

	return nil
}

// Dir is a Locker backed by lock files in a directory (O_EXCL create).
// Good enough for one operator box; use a distributed lock for teams.
type Dir struct {
	Path string
}

type dirRecord struct {
	Token  string    `json:"token"`
	Host   string    `json:"host"`
	PID    int       `json:"pid"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

func (d *Dir) file(key string) string {
	return filepath.Join(d.Path, strings.ReplaceAll(key, "/", "__")+".lock")
}

// Lock implements Locker.
func (d *Dir) Lock(_ context.Context, key, reason string) (Lock, error) {
	if err := os.MkdirAll(d.Path, 0o755); err != nil {
		return nil, err
	}

	name := d.file(key)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		holder, _ := os.ReadFile(name)

		return nil, fmt.Errorf("%w: %s (holder: %s; remove %s if stale)", ErrHeld, key, strings.TrimSpace(string(holder)), name)
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	host, _ := os.Hostname()
	rec := dirRecord{Token: token(), Host: host, PID: os.Getpid(), Reason: reason, At: time.Now().UTC()}

	if err := json.NewEncoder(f).Encode(rec); err != nil {
		_ = os.Remove(name)

		return nil, err
	}

	return &dirLock{file: name, token: rec.Token}, nil
}

type dirLock struct {
	file, token string
}

func (l *dirLock) Validate(context.Context) error {
	b, err := os.ReadFile(l.file)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLost, err)
	}

	var rec dirRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.Token != l.token {
		return fmt.Errorf("%w: %s now belongs to someone else", ErrLost, l.file)
	}

	return nil
}

func (l *dirLock) Unlock(ctx context.Context) error {
	if err := l.Validate(ctx); err != nil {
		return err
	}

	return os.Remove(l.file)
}
