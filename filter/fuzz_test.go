package filter

import "testing"

// FuzzParseSpec: whatever an operator types, ParseSpec must not panic, and
// anything it accepts must survive a String() round trip.
func FuzzParseSpec(f *testing.F) {
	for _, seed := range []string{
		"users/ALL", "users/-80,80-|teams", "mess*/4*", " | ", "a/b/c", "[/x", "users/",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in string) {
		spec, err := ParseSpec(in)
		if err != nil {
			return
		}
		again, err := ParseSpec(spec.String())
		if err != nil {
			t.Fatalf("round trip of %q via %q failed: %v", in, spec.String(), err)
		}
		if again.String() != spec.String() {
			t.Fatalf("round trip changed %q into %q", spec.String(), again.String())
		}
	})
}
