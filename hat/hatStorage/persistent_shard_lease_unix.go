//go:build aix || android || dragonfly || freebsd || hurd || illumos || ios || linux || netbsd || openbsd || solaris

package hatStorage

import (
	"errors"
	"os"
	"syscall"
)

func persistentShardLeaseLockSupported() bool { return true }

func lockPersistentShardLease(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockPersistentShardLease(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func persistentShardLeaseLockHeld(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}
