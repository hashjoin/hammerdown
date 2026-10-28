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
//
// A lock must never ask the operator to judge whether its holder is dead.
// At 4 a.m., with a dropped SSH session and a deadline, "remove it if
// stale" reads as "remove it". Prefer locks that expire with their holder
// (leases, sessions). Locks that can outlive their holder, like Dir,
// implement Breaker: they check liveness themselves, refuse to release a
// lock whose holder is running, and record every release.
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

// Liveness of a lock holder, as far as this machine can tell.
type Liveness string

// Liveness values.
const (
	Alive   Liveness = "running"
	Dead    Liveness = "not running"
	Unknown Liveness = "unknown: holder is on another host"
)

// Holder describes who holds a lock.
type Holder struct {
	Key       string
	Token     string
	Host      string
	PID       int
	Reason    string
	At        time.Time // when it was taken
	Heartbeat time.Time // last time the holder proved it still had it
	Liveness  Liveness
}

func (h *Holder) String() string {
	return fmt.Sprintf("pid %d on %s (%s), since %s, last heartbeat %s ago, reason %q",
		h.PID, h.Host, h.Liveness, h.At.Format(time.RFC3339),
		time.Since(h.Heartbeat).Round(time.Second), h.Reason)
}

// HeldError is returned, wrapping ErrHeld, by lockers that know who holds
// the lock.
type HeldError struct{ Holder *Holder }

func (e *HeldError) Error() string {
	return fmt.Sprintf("%v: %s by %s", ErrHeld, e.Holder.Key, e.Holder)
}

// Unwrap makes errors.Is(err, ErrHeld) work.
func (e *HeldError) Unwrap() error { return ErrHeld }

// Breaker is implemented by lockers whose locks can outlive their holder.
type Breaker interface {
	// Holder reports who holds key, or nil if nobody does.
	Holder(ctx context.Context, key string) (*Holder, error)
	// Break releases key, but only if it is still held by h (same token)
	// and h is not Alive. by and reason are recorded.
	Break(ctx context.Context, h *Holder, by, reason string) error
}

// Dir is a Locker backed by lock files in a directory (O_EXCL create).
// Good enough for one operator box; use a distributed lock for teams.
//
// Each lock file records host, pid and a token; its mtime is the holder's
// heartbeat, refreshed by every Validate. Dir implements Breaker: a lock
// whose pid is still running on this host is never released, and every
// release is appended to unlock.log in the directory.
type Dir struct {
	Path string
}

var _ Breaker = (*Dir)(nil)

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
func (d *Dir) Lock(ctx context.Context, key, reason string) (Lock, error) {
	if err := os.MkdirAll(d.Path, 0o755); err != nil {
		return nil, err
	}

	name := d.file(key)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		h, herr := d.Holder(ctx, key)
		switch {
		case herr != nil:
			return nil, fmt.Errorf("%w: %s: unreadable lock record %s: %w", ErrHeld, key, name, herr)
		case h == nil: // released between our create and our read: just retry
			return d.Lock(ctx, key, reason)
		}

		return nil, &HeldError{Holder: h}
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

// Holder implements Breaker.
func (d *Dir) Holder(_ context.Context, key string) (*Holder, error) {
	name := d.file(key)

	b, err := os.ReadFile(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(name)
	if err != nil {
		return nil, err
	}

	var rec dirRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	h := &Holder{
		Key: key, Token: rec.Token, Host: rec.Host, PID: rec.PID, Reason: rec.Reason,
		At: rec.At, Heartbeat: st.ModTime(), Liveness: Unknown,
	}
	if me, _ := os.Hostname(); me != "" && me == rec.Host {
		h.Liveness = liveness(rec.PID)
	}

	return h, nil
}

// Break implements Breaker.
func (d *Dir) Break(ctx context.Context, h *Holder, by, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("a reason is required to release a lock")
	}

	cur, err := d.Holder(ctx, h.Key)
	switch {
	case err != nil:
		return err
	case cur == nil:
		return fmt.Errorf("%s is not held", h.Key)
	case cur.Token != h.Token:
		return fmt.Errorf("%s changed hands since it was inspected; inspect it again", h.Key)
	case cur.Liveness == Alive:
		return fmt.Errorf("refusing to release %s: its holder is running (%s)", h.Key, cur)
	}

	entry, _ := json.Marshal(struct {
		At     time.Time `json:"at"`
		By     string    `json:"by"`
		Reason string    `json:"reason"`
		Key    string    `json:"key"`
		Holder *Holder   `json:"holder"`
	}{time.Now().UTC(), by, reason, h.Key, cur})

	logf, err := os.OpenFile(filepath.Join(d.Path, "unlock.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("recording the release: %w", err)
	}
	_, werr := logf.Write(append(entry, '\n'))
	if cerr := logf.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return fmt.Errorf("recording the release: %w", werr)
	}

	return os.Remove(d.file(h.Key))
}

type dirLock struct {
	file, token string
}

// Validate checks the lock is still ours and refreshes the heartbeat.
func (l *dirLock) Validate(context.Context) error {
	b, err := os.ReadFile(l.file)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLost, err)
	}

	var rec dirRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.Token != l.token {
		return fmt.Errorf("%w: %s now belongs to someone else", ErrLost, l.file)
	}

	now := time.Now()
	_ = os.Chtimes(l.file, now, now) // heartbeat; best effort

	return nil
}

func (l *dirLock) Unlock(ctx context.Context) error {
	if err := l.Validate(ctx); err != nil {
		return err
	}

	return os.Remove(l.file)
}
