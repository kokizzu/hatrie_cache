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
	clusterWriteCommitCoordinatorSnapshotMagic     = "HCCS1"
	clusterWriteCommitCoordinatorFileStoreMagic    = "HCCF1"
	clusterWriteCommitCoordinatorFileStoreHeader   = len(clusterWriteCommitCoordinatorFileStoreMagic) + 8 + 4
	clusterWriteCommitCoordinatorFileStoreMaxBytes = MaxClusterWriteCommitCoordinatorSnapshotBytes + clusterWriteCommitCoordinatorFileStoreHeader
)

var (
	ErrClusterWriteCommitCoordinatorFileStorePathEmpty       = errors.New("hatReplication: coordinator file-store path is empty")
	ErrClusterWriteCommitCoordinatorFileStoreOptionsInvalid  = errors.New("hatReplication: coordinator file-store options are invalid")
	ErrClusterWriteCommitCoordinatorFileStoreContextInvalid  = errors.New("hatReplication: coordinator file-store context is invalid")
	ErrClusterWriteCommitCoordinatorFileStorePayloadTooLarge = errors.New("hatReplication: coordinator file-store payload is too large")
	ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid = errors.New("hatReplication: coordinator file-store snapshot is invalid")
)

var clusterWriteCommitCoordinatorFileStoreCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ClusterWriteCommitCoordinatorFileStoreOptions configures one private,
// atomically replaced coordinator state file.
type ClusterWriteCommitCoordinatorFileStoreOptions struct {
	Path     string
	MaxBytes int
}

// ClusterWriteCommitCoordinatorFileStore persists coordinator snapshots with a
// bounded binary envelope, CRC32C validation, 0600 files, and 0700 directories.
type ClusterWriteCommitCoordinatorFileStore struct {
	path     string
	maxBytes int
}

// NewClusterWriteCommitCoordinatorFileStore creates a bounded coordinator
// state store. MaxBytes == 0 selects the 64 MiB default envelope limit.
func NewClusterWriteCommitCoordinatorFileStore(options ClusterWriteCommitCoordinatorFileStoreOptions) (*ClusterWriteCommitCoordinatorFileStore, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, ErrClusterWriteCommitCoordinatorFileStorePathEmpty
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = clusterWriteCommitCoordinatorFileStoreMaxBytes
	}
	if maxBytes < clusterWriteCommitCoordinatorFileStoreHeader || maxBytes > clusterWriteCommitCoordinatorFileStoreMaxBytes {
		return nil, ErrClusterWriteCommitCoordinatorFileStoreOptionsInvalid
	}
	return &ClusterWriteCommitCoordinatorFileStore{path: options.Path, maxBytes: maxBytes}, nil
}

// Save atomically replaces the coordinator snapshot and syncs its directory.
func (store *ClusterWriteCommitCoordinatorFileStore) Save(ctx context.Context, snapshot ClusterWriteCommitCoordinatorSnapshot) error {
	if store == nil {
		return ErrClusterWriteCommitCoordinatorFileStoreOptionsInvalid
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
		return err
	}
	payload, err := marshalClusterWriteCommitCoordinatorSnapshot(snapshot)
	if err != nil {
		return err
	}
	encoded, err := marshalClusterWriteCommitCoordinatorFileStore(payload, store.maxBytes)
	if err != nil {
		return err
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
		return err
	}
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".coordinator-state-*")
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
	if err := writeClusterWriteCommitCoordinatorFileStoreBytes(temporary, encoded); err != nil {
		return err
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
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
	return syncClusterWriteCommitCoordinatorFileStoreDirectory(directory)
}

// Load verifies and restores one coordinator snapshot. A missing file returns
// found=false, while corrupt data never produces a partially decoded state.
func (store *ClusterWriteCommitCoordinatorFileStore) Load(ctx context.Context) (snapshot ClusterWriteCommitCoordinatorSnapshot, found bool, err error) {
	if store == nil {
		return snapshot, false, ErrClusterWriteCommitCoordinatorFileStoreOptionsInvalid
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
		return snapshot, false, err
	}
	file, err := os.Open(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return snapshot, false, nil
	}
	if err != nil {
		return snapshot, false, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, int64(store.maxBytes)+1))
	if err != nil {
		return snapshot, false, err
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
		return snapshot, false, err
	}
	if len(payload) > store.maxBytes {
		return snapshot, false, ErrClusterWriteCommitCoordinatorFileStorePayloadTooLarge
	}
	inner, err := unmarshalClusterWriteCommitCoordinatorFileStore(payload, store.maxBytes)
	if err != nil {
		return snapshot, false, err
	}
	snapshot, err = unmarshalClusterWriteCommitCoordinatorSnapshot(inner)
	if err != nil {
		return ClusterWriteCommitCoordinatorSnapshot{}, false, errors.Join(ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid, err)
	}
	return snapshot, true, nil
}

// Remove deletes a completed state file. Missing files are already removed.
func (store *ClusterWriteCommitCoordinatorFileStore) Remove(ctx context.Context) error {
	if store == nil {
		return ErrClusterWriteCommitCoordinatorFileStoreOptionsInvalid
	}
	if err := validateClusterWriteCommitCoordinatorFileStoreContext(ctx); err != nil {
		return err
	}
	err := os.Remove(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncClusterWriteCommitCoordinatorFileStoreDirectory(filepath.Dir(store.path))
}

func validateClusterWriteCommitCoordinatorFileStoreContext(ctx context.Context) error {
	if ctx == nil {
		return ErrClusterWriteCommitCoordinatorFileStoreContextInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func marshalClusterWriteCommitCoordinatorFileStore(payload []byte, maxBytes int) ([]byte, error) {
	if len(payload) > maxBytes-clusterWriteCommitCoordinatorFileStoreHeader {
		return nil, ErrClusterWriteCommitCoordinatorFileStorePayloadTooLarge
	}
	encoded := make([]byte, len(clusterWriteCommitCoordinatorFileStoreMagic)+8+len(payload)+4)
	offset := copy(encoded, clusterWriteCommitCoordinatorFileStoreMagic)
	binary.BigEndian.PutUint64(encoded[offset:offset+8], uint64(len(payload)))
	offset += 8
	copy(encoded[offset:], payload)
	checksumOffset := offset + len(payload)
	binary.BigEndian.PutUint32(encoded[checksumOffset:], crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitCoordinatorFileStoreCRCTable))
	return encoded, nil
}

func unmarshalClusterWriteCommitCoordinatorFileStore(encoded []byte, maxBytes int) ([]byte, error) {
	if len(encoded) < clusterWriteCommitCoordinatorFileStoreHeader || len(encoded) > maxBytes || !bytes.Equal(encoded[:len(clusterWriteCommitCoordinatorFileStoreMagic)], []byte(clusterWriteCommitCoordinatorFileStoreMagic)) {
		return nil, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	offset := len(clusterWriteCommitCoordinatorFileStoreMagic)
	payloadLength := binary.BigEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	if payloadLength > uint64(len(encoded)-offset-4) || int(payloadLength) != len(encoded)-offset-4 {
		return nil, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	checksumOffset := offset + int(payloadLength)
	if binary.BigEndian.Uint32(encoded[checksumOffset:]) != crc32.Checksum(encoded[:checksumOffset], clusterWriteCommitCoordinatorFileStoreCRCTable) {
		return nil, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	return encoded[offset:checksumOffset], nil
}

func marshalClusterWriteCommitCoordinatorSnapshot(snapshot ClusterWriteCommitCoordinatorSnapshot) ([]byte, error) {
	normalized, err := normalizeClusterWriteCommitCoordinatorSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	snapshot = normalized
	payload := make([]byte, 0, len(clusterWriteCommitCoordinatorSnapshotMagic)+len(snapshot.Nodes)*16+len(snapshot.Attempts)*32)
	payload = append(payload, clusterWriteCommitCoordinatorSnapshotMagic...)
	payload = appendClusterWriteCommitCoordinatorUvarint(payload, uint64(len(snapshot.Nodes)))
	for _, node := range snapshot.Nodes {
		payload = appendClusterWriteCommitCoordinatorString(payload, node)
	}
	payload = appendClusterWriteCommitCoordinatorString(payload, snapshot.Proposal.TransactionID)
	payload = appendClusterWriteCommitCoordinatorUvarint(payload, snapshot.Proposal.Sequence)
	payload = appendClusterWriteCommitCoordinatorUvarint(payload, snapshot.Proposal.FenceToken)
	payload = append(payload, snapshot.Proposal.PayloadDigest[:]...)
	payload = append(payload, byte(snapshot.Phase))
	for _, attempt := range snapshot.Attempts {
		var flags byte
		if attempt.Prepared {
			flags |= 1 << 0
		}
		if attempt.Aborted {
			flags |= 1 << 1
		}
		if attempt.Committed {
			flags |= 1 << 2
		}
		payload = append(payload, flags)
		payload = appendClusterWriteCommitCoordinatorString(payload, attempt.PrepareError)
		payload = appendClusterWriteCommitCoordinatorString(payload, attempt.AbortError)
		payload = appendClusterWriteCommitCoordinatorString(payload, attempt.CommitError)
		if len(payload) > MaxClusterWriteCommitCoordinatorSnapshotBytes {
			return nil, ErrClusterWriteCommitCoordinatorFileStorePayloadTooLarge
		}
	}
	return payload, nil
}

func unmarshalClusterWriteCommitCoordinatorSnapshot(payload []byte) (ClusterWriteCommitCoordinatorSnapshot, error) {
	var snapshot ClusterWriteCommitCoordinatorSnapshot
	if len(payload) < len(clusterWriteCommitCoordinatorSnapshotMagic) || len(payload) > MaxClusterWriteCommitCoordinatorSnapshotBytes || !bytes.Equal(payload[:len(clusterWriteCommitCoordinatorSnapshotMagic)], []byte(clusterWriteCommitCoordinatorSnapshotMagic)) {
		return snapshot, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	offset := len(clusterWriteCommitCoordinatorSnapshotMagic)
	nodeCount, ok := readClusterWriteCommitCoordinatorUvarint(payload, &offset)
	if !ok || nodeCount == 0 || nodeCount > MaxClusterWriteCommitNodes {
		return snapshot, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	snapshot.Nodes = make([]string, int(nodeCount))
	for index := range snapshot.Nodes {
		node, ok := readClusterWriteCommitCoordinatorString(payload, &offset)
		if !ok {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		snapshot.Nodes[index] = node
	}
	transactionID, ok := readClusterWriteCommitCoordinatorString(payload, &offset)
	if !ok {
		return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	sequence, ok := readClusterWriteCommitCoordinatorUvarint(payload, &offset)
	if !ok {
		return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	fenceToken, ok := readClusterWriteCommitCoordinatorUvarint(payload, &offset)
	if !ok || len(payload)-offset < 33 {
		return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	var digest [32]byte
	copy(digest[:], payload[offset:offset+32])
	offset += 32
	snapshot.Proposal = ClusterWriteCommitProposal{TransactionID: transactionID, Sequence: sequence, FenceToken: fenceToken, PayloadDigest: digest}
	snapshot.Phase = ClusterWriteCommitCoordinatorPhase(payload[offset])
	offset++
	snapshot.Attempts = make([]ClusterWriteCommitAttempt, int(nodeCount))
	for index := range snapshot.Attempts {
		if offset >= len(payload) {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		flags := payload[offset]
		offset++
		if flags&^byte(0x07) != 0 {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		prepareError, ok := readClusterWriteCommitCoordinatorString(payload, &offset)
		if !ok {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		abortError, ok := readClusterWriteCommitCoordinatorString(payload, &offset)
		if !ok {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		commitError, ok := readClusterWriteCommitCoordinatorString(payload, &offset)
		if !ok {
			return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
		}
		snapshot.Attempts[index] = ClusterWriteCommitAttempt{
			Node:         snapshot.Nodes[index],
			Prepared:     flags&(1<<0) != 0,
			Aborted:      flags&(1<<1) != 0,
			Committed:    flags&(1<<2) != 0,
			PrepareError: prepareError,
			AbortError:   abortError,
			CommitError:  commitError,
		}
	}
	if offset != len(payload) {
		return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid
	}
	normalized, err := normalizeClusterWriteCommitCoordinatorSnapshot(snapshot)
	if err != nil {
		return ClusterWriteCommitCoordinatorSnapshot{}, errors.Join(ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid, err)
	}
	return normalized, nil
}

func appendClusterWriteCommitCoordinatorUvarint(payload []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	return append(payload, buffer[:binary.PutUvarint(buffer[:], value)]...)
}

func appendClusterWriteCommitCoordinatorString(payload []byte, value string) []byte {
	payload = appendClusterWriteCommitCoordinatorUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readClusterWriteCommitCoordinatorUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, read := binary.Uvarint(payload[*offset:])
	if read <= 0 {
		return 0, false
	}
	*offset += read
	return value, true
}

func readClusterWriteCommitCoordinatorString(payload []byte, offset *int) (string, bool) {
	length, ok := readClusterWriteCommitCoordinatorUvarint(payload, offset)
	if !ok || length > maxClusterWriteCommitCoordinatorStringBytes || length > uint64(len(payload)-*offset) {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}

func syncClusterWriteCommitCoordinatorFileStoreDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}

func writeClusterWriteCommitCoordinatorFileStoreBytes(writer io.Writer, payload []byte) error {
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
