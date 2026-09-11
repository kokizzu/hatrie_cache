//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package hatCache

import "errors"

func persistentStoreAvailableBytes(string) (uint64, error) {
	return 0, errors.New("filesystem free-space inspection is unsupported on this platform")
}
