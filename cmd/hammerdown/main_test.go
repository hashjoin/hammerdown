package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseStopAt(t *testing.T) {
	now := time.Date(2026, 10, 21, 20, 0, 0, 0, time.UTC)
	got, err := parseStopAt("90m", now)
	if err != nil || !got.Equal(now.Add(90*time.Minute)) {
		t.Errorf("duration: %v %v", got, err)
	}
	got, err = parseStopAt("2026-10-22T03:00:00Z", now)
	if err != nil || got.Hour() != 3 {
		t.Errorf("rfc3339: %v %v", got, err)
	}
	if _, err := parseStopAt("tomorrow", now); err == nil {
		t.Error("expected error")
	}
}

// End to end through the CLI on a fresh simulated fleet.
func TestCLISmoke(t *testing.T) {
	state := filepath.Join(t.TempDir(), "fleet.json")
	devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	stdout := os.Stdout
	os.Stdout = devnull
	defer func() { os.Stdout = stdout }()

	steps := [][]string{
		{"sim", "init", "--sim-state", state},
		{"sim", "stage", "--sim-state", state, "--to", "v20.0.1", "--limit", "users/ALL"},
		{"report", "--sim-state", state, "--json"},
		{"roll", "--sim-state", state, "--sim-latency", "1ms", "--workers", "32", "--dry-run=false", "--yes", "--quiet"},
	}
	for _, s := range steps {
		if code := run(s); code != exitOK {
			t.Fatalf("%v exited %d", s, code)
		}
	}
	if code := run([]string{"roll", "--limit", "[bad"}); code != exitUsage {
		t.Errorf("bad --limit exited %d, want %d", code, exitUsage)
	}
}
