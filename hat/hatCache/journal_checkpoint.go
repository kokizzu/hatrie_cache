package hatCache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const commandJournalCheckpointMagic = "hatrie-cache-command-journal-checkpoint/v1\n"

var (
	ErrNilCommandJournalCheckpointStore = errors.New("hatriecache: command journal checkpoint store is nil")
	ErrInvalidCommandJournalCheckpoint  = errors.New("hatriecache: command journal checkpoint is invalid")
)

// CommandJournalCheckpointStore persists the last sequence acknowledged by a
// consumer. Save must be durable before returning when the consumer uses the
// store for restart recovery.
type CommandJournalCheckpointStore interface {
	Load(context.Context) (uint64, error)
	Save(context.Context, uint64) error
}

// FileCommandJournalCheckpointStore atomically persists one journal sequence
// in a small checksummed file. A missing file represents the zero checkpoint.
type FileCommandJournalCheckpointStore struct {
	path string
}

// NewFileCommandJournalCheckpointStore creates a file-backed checkpoint store.
// The parent directory is created on the first Save, not during construction.
func NewFileCommandJournalCheckpointStore(path string) (*FileCommandJournalCheckpointStore, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: checkpoint path is empty", ErrInvalidCommandJournalCheckpoint)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint path: %w", err)
	}
	return &FileCommandJournalCheckpointStore{path: absolutePath}, nil
}

// Load returns zero when the checkpoint file does not exist.
func (store *FileCommandJournalCheckpointStore) Load(ctx context.Context) (uint64, error) {
	if store == nil {
		return 0, ErrNilCommandJournalCheckpointStore
	}
	if err := commandJournalCheckpointContextError(ctx); err != nil {
		return 0, err
	}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load command journal checkpoint: %w", err)
	}
	const sequenceBytes = 8
	const checksumBytes = 4
	const minimumBytes = len(commandJournalCheckpointMagic) + sequenceBytes + checksumBytes
	if len(data) != minimumBytes || !bytes.HasPrefix(data, []byte(commandJournalCheckpointMagic)) {
		return 0, fmt.Errorf("%w: unexpected file format", ErrInvalidCommandJournalCheckpoint)
	}
	payload := data[:len(data)-checksumBytes]
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-checksumBytes:])
	if gotChecksum := crc32.ChecksumIEEE(payload); gotChecksum != wantChecksum {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrInvalidCommandJournalCheckpoint)
	}
	return binary.LittleEndian.Uint64(payload[len(commandJournalCheckpointMagic):]), nil
}

// Save atomically replaces the checkpoint file and leaves a private file mode
// on both the temporary and final files.
func (store *FileCommandJournalCheckpointStore) Save(ctx context.Context, sequence uint64) error {
	if store == nil {
		return ErrNilCommandJournalCheckpointStore
	}
	if err := commandJournalCheckpointContextError(ctx); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o750); err != nil {
		return fmt.Errorf("create command journal checkpoint directory: %w", err)
	}
	payload := make([]byte, len(commandJournalCheckpointMagic)+8)
	copy(payload, commandJournalCheckpointMagic)
	binary.LittleEndian.PutUint64(payload[len(commandJournalCheckpointMagic):], sequence)
	data := make([]byte, len(payload)+4)
	copy(data, payload)
	binary.LittleEndian.PutUint32(data[len(payload):], crc32.ChecksumIEEE(payload))

	temporary, err := os.CreateTemp(filepath.Dir(store.path), ".command-journal-checkpoint-*")
	if err != nil {
		return fmt.Errorf("create command journal checkpoint temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect command journal checkpoint temporary file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write command journal checkpoint: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync command journal checkpoint: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close command journal checkpoint temporary file: %w", err)
	}
	if err := commandJournalCheckpointContextError(ctx); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace command journal checkpoint: %w", err)
	}
	directory, err := os.Open(filepath.Dir(store.path))
	if err != nil {
		return fmt.Errorf("open command journal checkpoint directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync command journal checkpoint directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close command journal checkpoint directory: %w", err)
	}
	return nil
}

func commandJournalCheckpointContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
