//go:build aix || android || dragonfly || freebsd || hurd || illumos || ios || linux || netbsd || openbsd || solaris

package hatSql

import (
	"errors"
	"os"
	"syscall"
)

func sqlMutationDependencyQueueLeaseSupported() bool { return true }

func lockSQLMutationDependencyQueueLease(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockSQLMutationDependencyQueueLease(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func sqlMutationDependencyQueueLeaseHeld(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}
