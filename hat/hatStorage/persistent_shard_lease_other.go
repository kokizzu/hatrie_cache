//go:build !aix && !android && !dragonfly && !freebsd && !hurd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris

package hatStorage

import "os"

func persistentShardLeaseLockSupported() bool { return false }

func lockPersistentShardLease(*os.File) error { return ErrPersistentShardLeaseUnsupported }

func unlockPersistentShardLease(*os.File) error { return nil }

func persistentShardLeaseLockHeld(error) bool { return false }
