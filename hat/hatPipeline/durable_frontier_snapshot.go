package hatPipeline

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

var (
	// ErrFrontierSnapshotStoreRequired indicates that no durable snapshot store
	// was supplied to a registry operation.
	ErrFrontierSnapshotStoreRequired = errors.New("hatPipeline: frontier snapshot store is required")
	// ErrFrontierSnapshotStorePathEmpty indicates that a file store has no path.
	ErrFrontierSnapshotStorePathEmpty = errors.New("hatPipeline: frontier snapshot store path is empty")
	// ErrFrontierSnapshotStoreOptionsInvalid indicates an invalid file-store
	// bound.
	ErrFrontierSnapshotStoreOptionsInvalid = errors.New("hatPipeline: frontier snapshot store options are invalid")
	// ErrFrontierSnapshotStorePayloadTooLarge indicates that a payload exceeds
	// the configured durable-store limit.
	ErrFrontierSnapshotStorePayloadTooLarge = errors.New("hatPipeline: frontier snapshot store payload is too large")
)

// FrontierSnapshotStore persists and loads one encoded frontier snapshot.
// A nil payload with a nil error from Load means that no snapshot exists.
// Implementations must return from Save only after the payload is durable
// according to their own persistence contract.
type FrontierSnapshotStore interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
}

// FrontierSnapshotFileStoreOptions configures a local durable snapshot file.
type FrontierSnapshotFileStoreOptions struct {
	// Path is the snapshot file path. Its parent directory must already exist.
	Path string
	// MaxBytes bounds both writes and reads. Zero uses the binary snapshot
	// format's maximum size.
	MaxBytes int
}

// FrontierSnapshotFileStore atomically replaces one private snapshot file.
// It does not create parent directories, so deployment code can control their
// ownership and permissions explicitly.
type FrontierSnapshotFileStore struct {
	path     string
	maxBytes int
}

// NewFrontierSnapshotFileStore creates a bounded local file store.
func NewFrontierSnapshotFileStore(options FrontierSnapshotFileStoreOptions) (*FrontierSnapshotFileStore, error) {
	if options.Path == "" {
		return nil, ErrFrontierSnapshotStorePathEmpty
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = maxFrontierSnapshotBytes
	}
	if maxBytes < 1 || maxBytes > maxFrontierSnapshotBytes {
		return nil, ErrFrontierSnapshotStoreOptionsInvalid
	}
	return &FrontierSnapshotFileStore{
		path:     options.Path,
		maxBytes: maxBytes,
	}, nil
}

// Save writes payload through a same-directory temporary file, flushes it,
// renames it into place, and flushes the parent directory. A canceled context
// never reports success, even if cancellation races with the final flush.
func (store *FrontierSnapshotFileStore) Save(ctx context.Context, payload []byte) error {
	if store == nil {
		return ErrFrontierSnapshotStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	if len(payload) > store.maxBytes {
		return ErrFrontierSnapshotStorePayloadTooLarge
	}

	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if len(payload) > 0 {
		written, err := temporary.Write(payload)
		if err != nil {
			_ = temporary.Close()
			return err
		}
		if written != len(payload) {
			_ = temporary.Close()
			return io.ErrShortWrite
		}
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	if err := syncFrontierSnapshotDirectory(directory); err != nil {
		return err
	}
	return frontierSnapshotContextErr(ctx)
}

// Load returns nil, nil when the snapshot file does not exist. Existing files
// are bounded before allocation and are returned for registry-level format
// validation.
func (store *FrontierSnapshotFileStore) Load(ctx context.Context) ([]byte, error) {
	if store == nil {
		return nil, ErrFrontierSnapshotStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return nil, err
	}
	file, err := os.Open(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 0 || info.Size() > int64(store.maxBytes) {
		return nil, ErrFrontierSnapshotStorePayloadTooLarge
	}
	payload := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, payload); err != nil {
		return nil, err
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return nil, err
	}
	return payload, nil
}

// SaveDurableSnapshot encodes and stores the current frontier state.
func (registry *FrontierRegistry) SaveDurableSnapshot(ctx context.Context, store FrontierSnapshotStore) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	if store == nil {
		return ErrFrontierSnapshotStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	payload, err := registry.MarshalSnapshot()
	if err != nil {
		return err
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return err
	}
	return store.Save(ctx, payload)
}

// RestoreDurableSnapshot loads one snapshot. It returns found=false for a
// missing file or when restore fails, and never mutates a registry unless the
// complete payload passes RestoreSnapshot validation.
func (registry *FrontierRegistry) RestoreDurableSnapshot(ctx context.Context, store FrontierSnapshotStore) (found bool, err error) {
	if registry == nil {
		return false, ErrFrontierClosed
	}
	if store == nil {
		return false, ErrFrontierSnapshotStoreRequired
	}
	if err := frontierSnapshotContextErr(ctx); err != nil {
		return false, err
	}
	payload, err := store.Load(ctx)
	if err != nil {
		return false, err
	}
	if payload == nil {
		return false, nil
	}
	if err := registry.RestoreSnapshot(payload); err != nil {
		return false, err
	}
	return true, nil
}

func frontierSnapshotContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func syncFrontierSnapshotDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
