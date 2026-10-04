//go:build aix || android || darwin || dragonfly || freebsd || hurd || illumos || ios || linux || netbsd || openbsd || solaris

package hatMappedPart

import (
	"os"
	"syscall"
)

func mapReadOnly(file *os.File, size int) ([]byte, error) {
	if size == 0 {
		return []byte{}, nil
	}
	return syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
}

func unmapReadOnly(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
