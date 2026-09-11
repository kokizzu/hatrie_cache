//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package hatCache

import (
	"fmt"
	"syscall"
)

func persistentStoreAvailableBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	blocks := uint64(stat.Bavail)
	blockSize := uint64(stat.Bsize)
	if blockSize == 0 || blocks > ^uint64(0)/blockSize {
		return 0, fmt.Errorf("invalid filesystem block geometry")
	}
	return blocks * blockSize, nil
}
