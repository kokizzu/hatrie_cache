//go:build !aix && !android && !dragonfly && !freebsd && !hurd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris

package hatSql

import "os"

func sqlMutationDependencyQueueLeaseSupported() bool { return false }

func lockSQLMutationDependencyQueueLease(*os.File) error {
	return ErrSQLMutationDependencyQueueLeaseUnsupported
}

func unlockSQLMutationDependencyQueueLease(*os.File) error { return nil }

func sqlMutationDependencyQueueLeaseHeld(error) bool { return false }
