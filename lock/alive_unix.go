//go:build unix

package lock

import (
	"errors"
	"syscall"
)

// liveness asks the kernel whether pid exists. EPERM means it does, just
// not ours to signal. A reused pid reads as Alive: the safe direction.
func liveness(pid int) Liveness {
	if pid <= 0 {
		return Dead
	}

	err := syscall.Kill(pid, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return Alive
	case errors.Is(err, syscall.ESRCH):
		return Dead
	default:
		return Unknown
	}
}
