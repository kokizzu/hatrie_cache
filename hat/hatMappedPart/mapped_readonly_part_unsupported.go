//go:build !aix && !android && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris

package hatMappedPart

import (
	"errors"
	"os"
)

func mapReadOnly(_ *os.File, _ int) ([]byte, error) {
	return nil, errors.New("file mapping unavailable")
}

func unmapReadOnly(_ []byte) error {
	return nil
}
