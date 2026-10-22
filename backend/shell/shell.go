// Package shell is the "bring your own scripts" backend: hammerdown does the
// scheduling, filtering, locking and fail-fast; your existing scripts do the
// work. It is the fastest way to point hammerdown at a real fleet.
//
//	--shell-inventory   command that prints the fleet as a JSON array of
//	                    fleet.Node objects on stdout
//	--shell-restart     command run per node; must block until the node is
//	                    healthy on its target build, exit 0 on success
//	--shell-failover    command run per shard; must block until the old
//	                    primary is demoted, exit 0 on success
//
// Commands run via `sh -c` and are Go text/templates. They also get HD_*
// environment variables, so plain scripts don't need templating at all:
//
//	--shell-restart 'ssh {{.Node.Host}} sudo systemctl restart mysqld'
//	--shell-failover './failover.sh'   # reads $HD_FROM_HOST $HD_TO_HOST
package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/hashjoin/hammerdown/backend"
	"github.com/hashjoin/hammerdown/fleet"
	"github.com/hashjoin/hammerdown/lock"
	"github.com/hashjoin/hammerdown/roll"
)

// Config holds the shell flags.
type Config struct {
	Inventory string
	Restart   string
	Failover  string
	Guard     string
	LockDir   string
	Timeout   time.Duration
	Jitter    time.Duration
}

// Factory is the shell backend.Factory.
type Factory struct{ cfg Config }

// NewFactory returns the shell factory.
func NewFactory() *Factory { return &Factory{} }

// Name implements backend.Factory.
func (*Factory) Name() string { return "shell" }

// Describe implements backend.Factory.
func (*Factory) Describe() string {
	return "drive your own scripts: inventory, restart, failover (and an optional per-shard guard)"
}

// RegisterFlags implements backend.Factory.
func (f *Factory) RegisterFlags(fs *flag.FlagSet) {
	home, _ := os.UserHomeDir()
	fs.StringVar(&f.cfg.Inventory, "shell-inventory", "", "shell: command printing the fleet as JSON")
	fs.StringVar(&f.cfg.Restart, "shell-restart", "", "shell: per-node restart command (template; blocks until healthy)")
	fs.StringVar(&f.cfg.Failover, "shell-failover", "", "shell: per-shard planned failover command (template)")
	fs.StringVar(&f.cfg.Guard, "shell-guard", "", "shell: optional per-shard pre-flight command; non-zero exit skips the shard")
	fs.StringVar(&f.cfg.LockDir, "shell-lock-dir", filepath.Join(home, ".hammerdown", "locks"), "shell: lock file directory")
	fs.DurationVar(&f.cfg.Timeout, "shell-timeout", 10*time.Minute, "shell: timeout for each command")
	fs.DurationVar(&f.cfg.Jitter, "shell-jitter", time.Second, "shell: jitter before each restart")
}

// Open implements backend.Factory.
func (f *Factory) Open(_ context.Context, log roll.Logger) (backend.Backend, error) {
	if f.cfg.Inventory == "" {
		return nil, errors.New("shell: --shell-inventory is required")
	}

	b := &shellBackend{cfg: f.cfg, log: log, locker: &lock.Dir{Path: f.cfg.LockDir}}

	var err error
	if b.restart, err = parse("restart", f.cfg.Restart); err != nil {
		return nil, err
	}
	if b.failover, err = parse("failover", f.cfg.Failover); err != nil {
		return nil, err
	}
	if b.guard, err = parse("guard", f.cfg.Guard); err != nil {
		return nil, err
	}

	return b, nil
}

func parse(name, src string) (*template.Template, error) {
	if src == "" {
		return nil, nil
	}
	t, err := template.New(name).Option("missingkey=error").Parse(src)
	if err != nil {
		return nil, fmt.Errorf("shell: --shell-%s: %w", name, err)
	}

	return t, nil
}

type shellBackend struct {
	cfg                      Config
	log                      roll.Logger
	locker                   lock.Locker
	restart, failover, guard *template.Template
}

// Data is what command templates see.
type Data struct {
	Node     *fleet.Node // restart: the node
	From, To *fleet.Node // failover: old and new primary
	Shard    *roll.Shard // failover, guard
}

func (b *shellBackend) Inventory() fleet.Inventory {
	return fleet.InventoryFunc(func(ctx context.Context) ([]*fleet.Node, error) {
		out, err := b.run(ctx, b.cfg.Inventory, nil)
		if err != nil {
			return nil, fmt.Errorf("shell inventory: %w", err)
		}
		var nodes []*fleet.Node
		if err := json.Unmarshal(out, &nodes); err != nil {
			return nil, fmt.Errorf("shell inventory: parsing JSON: %w", err)
		}

		return nodes, nil
	})
}

func (b *shellBackend) Processor() roll.Processor {
	return &roll.LeaderAware{Ops: b, Jitter: b.cfg.Jitter}
}

func (b *shellBackend) Locker() lock.Locker { return b.locker }

func (b *shellBackend) Guards() []roll.Guard {
	if b.guard == nil {
		return nil
	}

	return []roll.Guard{roll.GuardFunc(func(ctx context.Context, s *roll.Shard) error {
		cmd, err := render(b.guard, Data{Shard: s})
		if err != nil {
			return err
		}
		_, err = b.run(ctx, cmd, shardEnv(s))

		return err
	})}
}

func (b *shellBackend) Close(context.Context) error { return nil }

// Restart implements roll.NodeOps.
func (b *shellBackend) Restart(ctx context.Context, n *fleet.Node) error {
	if b.restart == nil {
		return errors.New("shell: --shell-restart not set")
	}
	cmd, err := render(b.restart, Data{Node: n})
	if err != nil {
		return err
	}
	if _, err := b.run(ctx, cmd, nodeEnv("HD_", n)); err != nil {
		return err
	}
	n.Running, n.State = n.Target, fleet.StateOK

	return nil
}

// Failover implements roll.NodeOps.
func (b *shellBackend) Failover(ctx context.Context, s *roll.Shard, from, to *fleet.Node) error {
	if b.failover == nil {
		return errors.New("shell: --shell-failover not set")
	}
	cmd, err := render(b.failover, Data{From: from, To: to, Shard: s})
	if err != nil {
		return err
	}
	env := append(shardEnv(s), append(nodeEnv("HD_FROM_", from), nodeEnv("HD_TO_", to)...)...)
	if _, err := b.run(ctx, cmd, env); err != nil {
		return err
	}
	from.Role, to.Role = fleet.Replica, fleet.Primary

	return nil
}

func render(t *template.Template, d Data) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, d); err != nil {
		return "", fmt.Errorf("shell: rendering %s: %w", t.Name(), err)
	}

	return buf.String(), nil
}

func (b *shellBackend) run(ctx context.Context, command string, env []string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	c := exec.CommandContext(ctx, "sh", "-c", command)
	c.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr

	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%q: %w: %s", command, err, tail(stderr.String(), 400))
	}

	return stdout.Bytes(), nil
}

func nodeEnv(prefix string, n *fleet.Node) []string {
	return []string{
		prefix + "NAME=" + n.Name,
		prefix + "HOST=" + n.Host,
		prefix + "KEYSPACE=" + n.Keyspace,
		prefix + "SHARD=" + n.Shard,
		prefix + "CELL=" + n.Cell,
		prefix + "ROLE=" + string(n.Role),
		prefix + "RUNNING=" + n.Running,
		prefix + "TARGET=" + n.Target,
	}
}

func shardEnv(s *roll.Shard) []string {
	return []string{"HD_KEYSPACE=" + s.Keyspace, "HD_SHARD=" + s.Name}
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}

	return s
}
