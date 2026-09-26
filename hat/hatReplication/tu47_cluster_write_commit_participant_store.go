package hatReplication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	clusterWriteCommitParticipantFileStoreMagic      = "HCPF1"
	clusterWriteCommitParticipantFileStoreHeaderSize = len(clusterWriteCommitParticipantFileStoreMagic) + 8 + 4
	clusterWriteCommitParticipantFileStoreDefaultMax = MaxClusterWriteCommitParticipantSnapshotBytes + clusterWriteCommitParticipantFileStoreHeaderSize
)

var (
	ErrClusterWriteCommitParticipantFileStorePathEmpty       = errors.New("hatReplication: participant file-store path is empty")
	ErrClusterWriteCommitParticipantFileStoreOptionsInvalid  = errors.New("hatReplication: participant file-store options are invalid")
	ErrClusterWriteCommitParticipantFileStoreContextInvalid  = errors.New("hatReplication: participant file-store context is invalid")
	ErrClusterWriteCommitParticipantFileStorePayloadTooLarge = errors.New("hatReplication: participant file-store payload is too large")
	ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid = errors.New("hatReplication: participant file-store snapshot is invalid")
)

var clusterWriteCommitParticipantFileStoreCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ClusterWriteCommitParticipantFileStoreOptions bounds one atomic participant
// state file. The file is created with private permissions and the parent
// directory is created with mode 0700 when it does not exist.
type ClusterWriteCommitParticipantFileStoreOptions struct {
	Path     string
	MaxBytes int
}

// ClusterWriteCommitParticipantFileStore persists participant state without
// changing the participant's prepare/commit/abort hot path. Callers opt into
// Save after their desired durability boundary and Load during recovery.
type ClusterWriteCommitParticipantFileStore struct {
	path     string
	maxBytes int
}

// NewClusterWriteCommitParticipantFileStore creates a bounded local file
// store. The envelope limit includes its magic, length, and CRC fields.
func NewClusterWriteCommitParticipantFileStore(options ClusterWriteCommitParticipantFileStoreOptions) (*ClusterWriteCommitParticipantFileStore, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, ErrClusterWriteCommitParticipantFileStorePathEmpty
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = clusterWriteCommitParticipantFileStoreDefaultMax
	}
	if maxBytes < 1 || maxBytes > clusterWriteCommitParticipantFileStoreDefaultMax {
		return nil, ErrClusterWriteCommitParticipantFileStoreOptionsInvalid
	}
	return &ClusterWriteCommitParticipantFileStore{path: options.Path, maxBytes: maxBytes}, nil
}

// Save snapshots participant and atomically replaces the store file. A
// canceled context observed before the filesystem operation prevents it.
func (store *ClusterWriteCommitParticipantFileStore) Save(ctx context.Context, participant *ClusterWriteCommitParticipant) error {
	if store == nil || participant == nil {
		return ErrClusterWriteCommitParticipantFileStoreOptionsInvalid
	}
	if err := validateClusterWriteCommitParticipantFileStoreContext(ctx); err != nil {
		return err
	}
	payload, err := participant.MarshalSnapshot()
	if err != nil {
		return err
	}
	encoded, err := marshalClusterWriteCommitParticipantFileStore(payload, store.maxBytes)
	if err != nil {
		return err
	}
	if err := validateClusterWriteCommitParticipantFileStoreContext(ctx); err != nil {
		return err
	}
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".participant-state-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if err := writeClusterWriteCommitParticipantFileStoreBytes(temporary, encoded); err != nil {
		return err
	}
	if err := validateClusterWriteCommitParticipantFileStoreContext(ctx); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	committed = true
	return syncClusterWriteCommitParticipantFileStoreDirectory(directory)
}

// Load verifies and atomically restores one participant snapshot. A missing
// file is reported as found=false, while a corrupt file never changes the
// participant supplied by the caller.
func (store *ClusterWriteCommitParticipantFileStore) Load(ctx context.Context, participant *ClusterWriteCommitParticipant) (found bool, err error) {
	if store == nil || participant == nil {
		return false, ErrClusterWriteCommitParticipantFileStoreOptionsInvalid
	}
	if err := validateClusterWriteCommitParticipantFileStoreContext(ctx); err != nil {
		return false, err
	}
	file, err := os.Open(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, int64(store.maxBytes)+1))
	if err != nil {
		return false, err
	}
	if err := validateClusterWriteCommitParticipantFileStoreContext(ctx); err != nil {
		return false, err
	}
	if len(payload) > store.maxBytes {
		return false, ErrClusterWriteCommitParticipantFileStorePayloadTooLarge
	}
	snapshot, err := unmarshalClusterWriteCommitParticipantFileStore(payload, store.maxBytes)
	if err != nil {
		return false, err
	}
	if err := participant.RestoreSnapshot(snapshot); err != nil {
		return false, errors.Join(ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid, err)
	}
	return true, nil
}

func validateClusterWriteCommitParticipantFileStoreContext(ctx context.Context) error {
	if ctx == nil {
		return ErrClusterWriteCommitParticipantFileStoreContextInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func marshalClusterWriteCommitParticipantFileStore(snapshot []byte, maxBytes int) ([]byte, error) {
	if len(snapshot) > maxBytes-clusterWriteCommitParticipantFileStoreHeaderSize {
		return nil, ErrClusterWriteCommitParticipantFileStorePayloadTooLarge
	}
	encoded := make([]byte, clusterWriteCommitParticipantFileStoreMagicSize()+8+len(snapshot)+4)
	offset := copy(encoded, clusterWriteCommitParticipantFileStoreMagic)
	binary.BigEndian.PutUint64(encoded[offset:offset+8], uint64(len(snapshot)))
	offset += 8
	copy(encoded[offset:], snapshot)
	checksumOffset := offset + len(snapshot)
	binary.BigEndian.PutUint32(encoded[checksumOffset:], crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitParticipantFileStoreCRCTable))
	return encoded, nil
}

func unmarshalClusterWriteCommitParticipantFileStore(encoded []byte, maxBytes int) ([]byte, error) {
	if len(encoded) < clusterWriteCommitParticipantFileStoreHeaderSize || len(encoded) > maxBytes || !bytes.Equal(encoded[:len(clusterWriteCommitParticipantFileStoreMagic)], []byte(clusterWriteCommitParticipantFileStoreMagic)) {
		return nil, ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid
	}
	offset := len(clusterWriteCommitParticipantFileStoreMagic)
	snapshotLength := binary.BigEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	if snapshotLength > uint64(len(encoded)-offset-4) || int(snapshotLength) != len(encoded)-offset-4 {
		return nil, ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid
	}
	checksumOffset := offset + int(snapshotLength)
	if binary.BigEndian.Uint32(encoded[checksumOffset:]) != crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitParticipantFileStoreCRCTable) {
		return nil, ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid
	}
	return encoded[offset:checksumOffset], nil
}

func clusterWriteCommitParticipantFileStoreMagicSize() int {
	return len(clusterWriteCommitParticipantFileStoreMagic)
}

func syncClusterWriteCommitParticipantFileStoreDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}

func writeClusterWriteCommitParticipantFileStoreBytes(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
