package hatSql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const projectionCheckpointFileMode = 0o600

// FileProjectionCheckpointStore persists named projection checkpoints in one
// local JSON file. It uses same-directory atomic replacement and rejects
// symlinks so a configured checkpoint path cannot redirect writes elsewhere.
type FileProjectionCheckpointStore struct {
	mu   sync.Mutex
	path string
}

// NewFileProjectionCheckpointStore creates a durable checkpoint store at path.
// The parent directory must already exist when a checkpoint is first saved.
func NewFileProjectionCheckpointStore(path string) (*FileProjectionCheckpointStore, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "." {
		return nil, fmt.Errorf("projection checkpoint path is required")
	}
	return &FileProjectionCheckpointStore{path: filepath.Clean(path)}, nil
}

// LoadProjectionCheckpoint loads one durable sequence. A missing checkpoint
// file is an empty store; malformed or insecure files are reported as errors.
func (store *FileProjectionCheckpointStore) LoadProjectionCheckpoint(ctx context.Context, name string) (uint64, bool, error) {
	if store == nil {
		return 0, false, fmt.Errorf("file projection checkpoint store is nil")
	}
	if err := projectionCheckpointContextError(ctx); err != nil {
		return 0, false, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false, fmt.Errorf("projection checkpoint name is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoints, found, err := store.loadLocked()
	if err != nil || !found {
		return 0, false, err
	}
	sequence, found := checkpoints[name]
	return sequence, found, nil
}

// SaveProjectionCheckpoint atomically persists one projection sequence.
func (store *FileProjectionCheckpointStore) SaveProjectionCheckpoint(ctx context.Context, name string, sequence uint64) error {
	if store == nil {
		return fmt.Errorf("file projection checkpoint store is nil")
	}
	if err := projectionCheckpointContextError(ctx); err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("projection checkpoint name is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoints, _, err := store.loadLocked()
	if err != nil {
		return err
	}
	if checkpoints == nil {
		checkpoints = map[string]uint64{}
	}
	checkpoints[name] = sequence
	data, err := json.Marshal(checkpoints)
	if err != nil {
		return fmt.Errorf("marshal projection checkpoints: %w", err)
	}
	if err := projectionCheckpointContextError(ctx); err != nil {
		return err
	}
	return store.writeLocked(data)
}

func (store *FileProjectionCheckpointStore) loadLocked() (map[string]uint64, bool, error) {
	info, err := os.Lstat(store.path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect projection checkpoint file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("projection checkpoint path must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, false, fmt.Errorf("projection checkpoint file permissions must not grant group or other access")
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		return nil, false, fmt.Errorf("read projection checkpoint file: %w", err)
	}
	checkpoints := map[string]uint64{}
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return nil, false, fmt.Errorf("decode projection checkpoint file: %w", err)
	}
	return checkpoints, true, nil
}

func (store *FileProjectionCheckpointStore) writeLocked(data []byte) error {
	directory := filepath.Dir(store.path)
	info, err := os.Stat(directory)
	if err != nil {
		return fmt.Errorf("inspect projection checkpoint directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("projection checkpoint parent is not a directory")
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary projection checkpoint file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(projectionCheckpointFileMode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set projection checkpoint permissions: %w", err)
	}
	if written, err := temporary.Write(data); err != nil || written != len(data) {
		_ = temporary.Close()
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("write projection checkpoint file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync projection checkpoint file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close projection checkpoint file: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace projection checkpoint file: %w", err)
	}
	removeTemporary = false
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open projection checkpoint directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync projection checkpoint directory: %w", err)
	}
	return nil
}

func projectionCheckpointContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("projection checkpoint context: %w", err)
	}
	return nil
}
