package hatStorage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const backendMarkerSuffix = ".backend"

// BackendMarkerSuffix is appended to a data directory to name its backend
// selection marker.
const BackendMarkerSuffix = backendMarkerSuffix

// ResolveBackend resolves requested against a durable engine marker. For an
// unmarked non-empty directory it preserves the legacy LevelDB default.
func ResolveBackend(path string, requested Backend) (Backend, error) {
	requested, err := ParseBackend(string(requested))
	if err != nil {
		return "", err
	}
	marked, hasMarker, err := ReadBackendMarker(path)
	if err != nil {
		return "", err
	}
	if hasMarker {
		if requested != BackendAuto && requested != marked {
			return "", fmt.Errorf("hatriecache: storage backend %q does not match %q marker", requested, marked)
		}
		return marked, nil
	}
	if requested != BackendAuto {
		return requested, nil
	}
	entries, err := os.ReadDir(path)
	if err == nil && len(entries) > 0 {
		return BackendLevelDB, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return DefaultBackend, nil
}

// BackendMarkerPath returns the durable marker path for a storage directory.
func BackendMarkerPath(path string) string {
	return path + backendMarkerSuffix
}

// ReadBackendMarker reads and validates the optional durable engine marker.
func ReadBackendMarker(path string) (Backend, bool, error) {
	data, err := os.ReadFile(BackendMarkerPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	backend, err := ParseBackend(string(data))
	if err != nil || backend == BackendAuto {
		return "", false, fmt.Errorf("hatriecache: invalid storage backend marker %q", strings.TrimSpace(string(data)))
	}
	return backend, true, nil
}

// WriteBackendMarker atomically persists backend selection for path.
func WriteBackendMarker(path string, backend Backend) error {
	return WriteFileAtomic(BackendMarkerPath(path), []byte(string(backend)+"\n"))
}

// WriteFileAtomic atomically publishes a file and synchronizes its directory.
func WriteFileAtomic(path string, data []byte) error {
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		_, err := writer.Write(data)
		return err
	})
}

func writeFileAtomicStream(path string, write func(io.Writer) error) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	buffered := bufio.NewWriter(tmp)
	if err := write(buffered); err != nil {
		cleanup()
		return err
	}
	if err := buffered.Flush(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
