package roll

import (
	"fmt"
	"strings"
	"time"
)

// Window is a daily wall-clock window, e.g. "06:00-11:00" in
// America/Los_Angeles. Windows may cross midnight ("22:00-02:00").
//
// hammerdown uses it as a blackout: no NEW shard starts while the clock is
// inside the window (peak traffic, backups, the on-call handoff ...).
type Window struct {
	Start, End time.Duration // offset from local midnight
	Loc        *time.Location
	raw        string
}

// ParseWindow parses "HH:MM-HH:MM" in the given IANA zone ("" = Local).
func ParseWindow(s, tz string) (*Window, error) {
	loc := time.Local
	if tz != "" {
		var err error
		if loc, err = time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("window time zone %q: %w", tz, err)
		}
	}

	a, b, ok := strings.Cut(strings.TrimSpace(s), "-")
	if !ok {
		return nil, fmt.Errorf("window %q: want HH:MM-HH:MM", s)
	}

	start, err := clock(a)
	if err != nil {
		return nil, fmt.Errorf("window %q: %w", s, err)
	}
	end, err := clock(b)
	if err != nil {
		return nil, fmt.Errorf("window %q: %w", s, err)
	}

	return &Window{Start: start, End: end, Loc: loc, raw: s + " " + loc.String()}, nil
}

func clock(s string) (time.Duration, error) {
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}

	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute, nil
}

// Contains reports whether t falls inside the window.
func (w *Window) Contains(t time.Time) bool {
	t = t.In(w.Loc)
	off := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second

	if w.Start <= w.End {
		return off >= w.Start && off < w.End
	}

	// crosses midnight
	return off >= w.Start || off < w.End
}

// String renders the window as "HH:MM-HH:MM Zone".
func (w *Window) String() string { return w.raw }
