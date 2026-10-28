//go:build !unix

package lock

// liveness: no portable pid probe here, so never claim a holder is dead.
func liveness(int) Liveness { return Unknown }
