package hatTopology

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	durablePartitionOwnershipMagic       = "HPO1"
	durablePartitionOwnershipVersion     = byte(1)
	durablePartitionOwnershipMaxBytes    = 1 << 20
	durablePartitionOwnershipMaxString   = 64 << 10
	durablePartitionOwnershipMaxListSize = 1024
)

var (
	// ErrDurablePartitionOwnershipInvalid reports malformed metadata, paths, or
	// consensus decisions.
	ErrDurablePartitionOwnershipInvalid = errors.New("hatriecache: durable partition ownership is invalid")
	// ErrDurablePartitionOwnershipChecksum reports a corrupted persisted record.
	ErrDurablePartitionOwnershipChecksum = errors.New("hatriecache: durable partition ownership checksum mismatch")
	// ErrDurablePartitionOwnershipSequence reports a non-monotone consensus
	// sequence.
	ErrDurablePartitionOwnershipSequence = errors.New("hatriecache: durable partition ownership sequence is not monotone")
	// ErrDurablePartitionOwnershipFrontier reports a regressed source frontier.
	ErrDurablePartitionOwnershipFrontier = errors.New("hatriecache: durable partition ownership frontier regressed")
	// ErrDurablePartitionOwnershipStale reports a decision fenced by newer
	// ownership metadata.
	ErrDurablePartitionOwnershipStale = errors.New("hatriecache: durable partition ownership decision is stale")
	// ErrDurablePartitionOwnershipShard reports a decision for another shard.
	ErrDurablePartitionOwnershipShard = errors.New("hatriecache: durable partition ownership shard changed")
)

var durablePartitionOwnershipCRCTable = crc32.MakeTable(crc32.Castagnoli)

// DurablePartitionOwnershipRecord is the crash-recoverable consensus result
// for one state shard. Sequence is the caller-owned consensus commit index;
// Frontier is the source frontier certified by that decision.
type DurablePartitionOwnershipRecord struct {
	Sequence uint64                              `json:"sequence"`
	Frontier uint64                              `json:"frontier"`
	Decision PartitionOwnershipConsensusDecision `json:"decision"`
}

// DurablePartitionOwnershipStore atomically publishes one shard's ownership
// record. It is process-safe; callers must pair it with a persistent shard
// lease or another cross-process single-writer authority.
type DurablePartitionOwnershipStore struct {
	mu     sync.Mutex
	path   string
	record DurablePartitionOwnershipRecord
	loaded bool
}

// OpenDurablePartitionOwnershipStore opens or creates the logical store at
// path. A missing file is an empty store; malformed or corrupted files fail
// closed and are never treated as empty state.
func OpenDurablePartitionOwnershipStore(path string) (*DurablePartitionOwnershipStore, error) {
	path, err := normalizeDurablePartitionOwnershipPath(path)
	if err != nil {
		return nil, err
	}
	store := &DurablePartitionOwnershipStore{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read durable partition ownership: %w", err)
	}
	record, err := unmarshalDurablePartitionOwnership(data)
	if err != nil {
		return nil, err
	}
	store.record = record
	store.loaded = true
	return store, nil
}

// Snapshot returns an independently owned record and whether one has been
// committed. It is safe to call concurrently with Commit.
func (store *DurablePartitionOwnershipStore) Snapshot() (DurablePartitionOwnershipRecord, bool) {
	if store == nil {
		return DurablePartitionOwnershipRecord{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.loaded {
		return DurablePartitionOwnershipRecord{}, false
	}
	return cloneDurablePartitionOwnershipRecord(store.record), true
}

// Commit validates and atomically publishes one satisfied consensus decision.
// Sequence must increase strictly, Frontier cannot regress, and ownership
// changes require a newer fencing token. The caller owns cross-process writer
// serialization, normally through PersistentShardLease.
func (store *DurablePartitionOwnershipStore) Commit(sequence, frontier uint64, decision PartitionOwnershipConsensusDecision) (DurablePartitionOwnershipRecord, error) {
	if store == nil {
		return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if sequence == 0 {
		return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipSequence
	}
	if err := ValidatePartitionOwnershipConsensusDecision(decision, decision.Ownership); err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	if store.loaded {
		current := store.record
		if sequence <= current.Sequence {
			return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipSequence
		}
		if frontier < current.Frontier {
			return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipFrontier
		}
		if decision.Ownership.ShardID != current.Decision.Ownership.ShardID {
			return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipShard
		}
		if decision.Ownership.FencingToken < current.Decision.Ownership.FencingToken {
			return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipStale
		}
		if decision.Ownership.FencingToken == current.Decision.Ownership.FencingToken && !partitionOwnershipConsensusMetadataEqual(decision.Ownership, current.Decision.Ownership) {
			return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipStale
		}
	}
	record := DurablePartitionOwnershipRecord{
		Sequence: sequence,
		Frontier: frontier,
		Decision: clonePartitionOwnershipConsensusDecision(decision),
	}
	data, err := marshalDurablePartitionOwnership(record)
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	if err := writeDurablePartitionOwnership(store.path, data); err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	store.record = record
	store.loaded = true
	return cloneDurablePartitionOwnershipRecord(record), nil
}

func normalizeDurablePartitionOwnershipPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return "", ErrDurablePartitionOwnershipInvalid
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%w: normalize path: %v", ErrDurablePartitionOwnershipInvalid, err)
	}
	return filepath.Clean(abs), nil
}

func cloneDurablePartitionOwnershipRecord(record DurablePartitionOwnershipRecord) DurablePartitionOwnershipRecord {
	record.Decision = clonePartitionOwnershipConsensusDecision(record.Decision)
	return record
}

func clonePartitionOwnershipConsensusDecision(decision PartitionOwnershipConsensusDecision) PartitionOwnershipConsensusDecision {
	decision.Ownership.Replicas = append([]string(nil), decision.Ownership.Replicas...)
	decision.Voters = append([]string(nil), decision.Voters...)
	decision.Acknowledged = append([]string(nil), decision.Acknowledged...)
	decision.Rejected = append([]string(nil), decision.Rejected...)
	return decision
}

func marshalDurablePartitionOwnership(record DurablePartitionOwnershipRecord) ([]byte, error) {
	if err := ValidatePartitionOwnershipConsensusDecision(record.Decision, record.Decision.Ownership); err != nil {
		return nil, err
	}
	if record.Sequence == 0 {
		return nil, ErrDurablePartitionOwnershipSequence
	}
	payload := make([]byte, 0, 256)
	payload = append(payload, durablePartitionOwnershipMagic...)
	payload = append(payload, durablePartitionOwnershipVersion)
	payload = appendUvarint(payload, record.Sequence)
	payload = appendUvarint(payload, record.Frontier)
	ownership := record.Decision.Ownership
	payload = appendUvarint(payload, uint64(ownership.ShardID))
	payload = appendUvarint(payload, ownership.FencingToken)
	var err error
	payload, err = appendBoundedString(payload, ownership.Primary)
	if err != nil {
		return nil, err
	}
	payload, err = appendBoundedString(payload, ownership.TopologyFingerprint)
	if err != nil {
		return nil, err
	}
	if payload, err = appendBoundedStrings(payload, ownership.Replicas); err != nil {
		return nil, err
	}
	if payload, err = appendBoundedStrings(payload, record.Decision.Voters); err != nil {
		return nil, err
	}
	if record.Decision.Required < 1 || record.Decision.Required > durablePartitionOwnershipMaxListSize {
		return nil, fmt.Errorf("%w: required voter count", ErrDurablePartitionOwnershipInvalid)
	}
	payload = appendUvarint(payload, uint64(record.Decision.Required))
	if payload, err = appendBoundedStrings(payload, record.Decision.Acknowledged); err != nil {
		return nil, err
	}
	if payload, err = appendBoundedStrings(payload, record.Decision.Rejected); err != nil {
		return nil, err
	}
	if len(payload)+4 > durablePartitionOwnershipMaxBytes {
		return nil, fmt.Errorf("%w: record exceeds %d bytes", ErrDurablePartitionOwnershipInvalid, durablePartitionOwnershipMaxBytes)
	}
	checksum := crc32.Checksum(payload, durablePartitionOwnershipCRCTable)
	result := make([]byte, len(payload)+4)
	copy(result, payload)
	binary.BigEndian.PutUint32(result[len(payload):], checksum)
	return result, nil
}

func unmarshalDurablePartitionOwnership(data []byte) (DurablePartitionOwnershipRecord, error) {
	if len(data) < len(durablePartitionOwnershipMagic)+1+4 || len(data) > durablePartitionOwnershipMaxBytes {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: record length", ErrDurablePartitionOwnershipInvalid)
	}
	body := data[:len(data)-4]
	wantChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(body, durablePartitionOwnershipCRCTable) != wantChecksum {
		return DurablePartitionOwnershipRecord{}, ErrDurablePartitionOwnershipChecksum
	}
	if string(body[:len(durablePartitionOwnershipMagic)]) != durablePartitionOwnershipMagic || body[len(durablePartitionOwnershipMagic)] != durablePartitionOwnershipVersion {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: magic or version", ErrDurablePartitionOwnershipInvalid)
	}
	reader := durablePartitionOwnershipReader{data: body, offset: len(durablePartitionOwnershipMagic) + 1}
	sequence, err := reader.uvarint()
	if err != nil || sequence == 0 {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: sequence", ErrDurablePartitionOwnershipInvalid)
	}
	frontier, err := reader.uvarint()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: frontier", ErrDurablePartitionOwnershipInvalid)
	}
	shard, err := reader.uvarint()
	if err != nil || shard > uint64(^uint32(0)) {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: shard", ErrDurablePartitionOwnershipInvalid)
	}
	fencingToken, err := reader.uvarint()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: fencing token", ErrDurablePartitionOwnershipInvalid)
	}
	primary, err := reader.string()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	fingerprint, err := reader.string()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	replicas, err := reader.strings()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	voters, err := reader.strings()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	required, err := reader.uvarint()
	if err != nil || required < 1 || required > durablePartitionOwnershipMaxListSize {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: required voter count", ErrDurablePartitionOwnershipInvalid)
	}
	acknowledged, err := reader.strings()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	rejected, err := reader.strings()
	if err != nil {
		return DurablePartitionOwnershipRecord{}, err
	}
	if reader.offset != len(body) {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: trailing bytes", ErrDurablePartitionOwnershipInvalid)
	}
	record := DurablePartitionOwnershipRecord{
		Sequence: sequence,
		Frontier: frontier,
		Decision: PartitionOwnershipConsensusDecision{
			Ownership: PartitionOwnership{
				ShardID:             uint32(shard),
				Primary:             primary,
				Replicas:            replicas,
				TopologyFingerprint: fingerprint,
				FencingToken:        fencingToken,
			},
			Voters:       voters,
			Required:     int(required),
			Acknowledged: acknowledged,
			Rejected:     rejected,
			Satisfied:    len(acknowledged) >= int(required),
		},
	}
	if err := ValidatePartitionOwnershipConsensusDecision(record.Decision, record.Decision.Ownership); err != nil {
		return DurablePartitionOwnershipRecord{}, fmt.Errorf("%w: decision: %v", ErrDurablePartitionOwnershipInvalid, err)
	}
	return record, nil
}

func appendUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(destination, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendBoundedString(destination []byte, value string) ([]byte, error) {
	if len(value) > durablePartitionOwnershipMaxString || strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return nil, fmt.Errorf("%w: string field", ErrDurablePartitionOwnershipInvalid)
	}
	destination = appendUvarint(destination, uint64(len(value)))
	return append(destination, value...), nil
}

func appendBoundedStrings(destination []byte, values []string) ([]byte, error) {
	if len(values) > durablePartitionOwnershipMaxListSize {
		return nil, fmt.Errorf("%w: list length", ErrDurablePartitionOwnershipInvalid)
	}
	destination = appendUvarint(destination, uint64(len(values)))
	for _, value := range values {
		var err error
		destination, err = appendBoundedString(destination, value)
		if err != nil {
			return nil, err
		}
	}
	return destination, nil
}

type durablePartitionOwnershipReader struct {
	data   []byte
	offset int
}

func (reader *durablePartitionOwnershipReader) uvarint() (uint64, error) {
	if reader.offset >= len(reader.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, size := binary.Uvarint(reader.data[reader.offset:])
	if size <= 0 {
		return 0, fmt.Errorf("%w: invalid varint", ErrDurablePartitionOwnershipInvalid)
	}
	reader.offset += size
	return value, nil
}

func (reader *durablePartitionOwnershipReader) string() (string, error) {
	length, err := reader.uvarint()
	if err != nil || length > durablePartitionOwnershipMaxString || length > uint64(len(reader.data)-reader.offset) {
		return "", fmt.Errorf("%w: string field", ErrDurablePartitionOwnershipInvalid)
	}
	value := string(reader.data[reader.offset : reader.offset+int(length)])
	reader.offset += int(length)
	if strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: string field", ErrDurablePartitionOwnershipInvalid)
	}
	return value, nil
}

func (reader *durablePartitionOwnershipReader) strings() ([]string, error) {
	count, err := reader.uvarint()
	if err != nil || count > durablePartitionOwnershipMaxListSize {
		return nil, fmt.Errorf("%w: list length", ErrDurablePartitionOwnershipInvalid)
	}
	values := make([]string, 0, int(count))
	for index := uint64(0); index < count; index++ {
		value, err := reader.string()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func writeDurablePartitionOwnership(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create durable partition ownership directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-ownership-*")
	if err != nil {
		return fmt.Errorf("create durable partition ownership: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set durable partition ownership permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write durable partition ownership: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync durable partition ownership: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close durable partition ownership: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish durable partition ownership: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open durable partition ownership directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	return errors.Join(syncErr, closeErr)
}
