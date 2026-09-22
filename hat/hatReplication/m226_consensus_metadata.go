package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	shardConsensusMetadataFixedBytes = 5 + 8*6
	// MaxShardConsensusMetadataIdentityBytes bounds shard, owner, and vote IDs.
	MaxShardConsensusMetadataIdentityBytes = 256
	maxShardConsensusMetadataSnapshotBytes = shardConsensusMetadataFixedBytes + 2*3 + 3*MaxShardConsensusMetadataIdentityBytes
	shardConsensusMetadataVersion          = 1
)

var (
	// ErrShardConsensusMetadataNil indicates a method call on a nil store.
	ErrShardConsensusMetadataNil = errors.New("hatriecache: shard consensus metadata store is nil")
	// ErrShardConsensusMetadataInvalid identifies malformed metadata or an
	// invalid monotone transition.
	ErrShardConsensusMetadataInvalid = errors.New("hatriecache: shard consensus metadata is invalid")
	// ErrShardConsensusMetadataStale identifies a lower fencing, term, index,
	// frontier, or configuration generation value.
	ErrShardConsensusMetadataStale = errors.New("hatriecache: shard consensus metadata is stale")
	// ErrShardConsensusMetadataOwnerConflict identifies an owner change without
	// a new fencing token.
	ErrShardConsensusMetadataOwnerConflict = errors.New("hatriecache: shard consensus metadata owner conflicts with fencing token")
	// ErrShardConsensusMetadataVoteConflict identifies a second vote in one
	// term.
	ErrShardConsensusMetadataVoteConflict = errors.New("hatriecache: shard consensus metadata vote conflicts with term")
	// ErrShardConsensusMetadataSnapshotInvalid identifies a corrupt snapshot.
	ErrShardConsensusMetadataSnapshotInvalid = errors.New("hatriecache: shard consensus metadata snapshot is invalid")
)

// ShardConsensusMetadata is the durable monotone control record for one state
// shard. Owner and FencingToken come from ShardLeaseRegistry; Term and
// VotedFor are Raft-style election metadata; the remaining fields couple log,
// applied state, source frontier, and membership-generation progress.
type ShardConsensusMetadata struct {
	Shard                   string `json:"shard"`
	Owner                   string `json:"owner,omitempty"`
	FencingToken            uint64 `json:"fencing_token,omitempty"`
	Term                    uint64 `json:"term"`
	VotedFor                string `json:"voted_for,omitempty"`
	CommitIndex             uint64 `json:"commit_index"`
	AppliedIndex            uint64 `json:"applied_index"`
	Frontier                uint64 `json:"frontier"`
	ConfigurationGeneration uint64 `json:"configuration_generation"`
}

// ShardConsensusMetadataStore owns one shard's current metadata and accepts a
// record only when ownership and every monotone field are valid. It is
// process-local; callers persist MarshalBinary output atomically beside their
// shard state and restore it before accepting new work.
type ShardConsensusMetadataStore struct {
	mu      sync.RWMutex
	shard   string
	current ShardConsensusMetadata
}

// NewShardConsensusMetadataStore creates an empty metadata store for shard.
// The empty snapshot has no owner, term, or vote and cannot commit state until
// a valid lease-backed record is supplied.
func NewShardConsensusMetadataStore(shard string) (*ShardConsensusMetadataStore, error) {
	shard, err := normalizeShardConsensusMetadataIdentity(shard, true)
	if err != nil {
		return nil, err
	}
	return &ShardConsensusMetadataStore{
		shard:   shard,
		current: ShardConsensusMetadata{Shard: shard},
	}, nil
}

// NewShardConsensusMetadataStoreFromSnapshot creates a store from persisted
// metadata after validating the complete record.
func NewShardConsensusMetadataStoreFromSnapshot(snapshot ShardConsensusMetadata) (*ShardConsensusMetadataStore, error) {
	normalized, err := normalizeShardConsensusMetadata(snapshot)
	if err != nil {
		return nil, err
	}
	return &ShardConsensusMetadataStore{shard: normalized.Shard, current: normalized}, nil
}

// Commit atomically publishes metadata for the configured shard. A larger
// fencing token may transfer ownership; equal tokens must retain the same
// owner. Terms, indexes, frontiers, and configuration generations never move
// backward, and a term can record at most one vote.
func (store *ShardConsensusMetadataStore) Commit(metadata ShardConsensusMetadata) (ShardConsensusMetadata, error) {
	if store == nil {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataNil
	}
	normalized, err := normalizeShardConsensusMetadata(metadata)
	if err != nil {
		return ShardConsensusMetadata{}, err
	}
	if normalized.Owner == "" || normalized.FencingToken == 0 || normalized.Term == 0 {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: commit requires owner, fencing token, and term", ErrShardConsensusMetadataInvalid)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if normalized.Shard != store.shard {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: shard does not match store", ErrShardConsensusMetadataInvalid)
	}
	current := store.current
	if normalized.FencingToken < current.FencingToken {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataStale
	}
	if normalized.FencingToken == current.FencingToken && current.FencingToken != 0 && normalized.Owner != current.Owner {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataOwnerConflict
	}
	if normalized.Term < current.Term || normalized.CommitIndex < current.CommitIndex || normalized.AppliedIndex < current.AppliedIndex || normalized.Frontier < current.Frontier || normalized.ConfigurationGeneration < current.ConfigurationGeneration {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataStale
	}
	if normalized.Term == current.Term && current.VotedFor != "" && normalized.VotedFor != current.VotedFor {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataVoteConflict
	}
	store.current = normalized
	return normalized, nil
}

// Snapshot returns a detached current metadata record.
func (store *ShardConsensusMetadataStore) Snapshot() ShardConsensusMetadata {
	if store == nil {
		return ShardConsensusMetadata{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.current
}

// Restore atomically replaces the current metadata after validation. A failed
// restore leaves the previous record untouched.
func (store *ShardConsensusMetadataStore) Restore(metadata ShardConsensusMetadata) error {
	if store == nil {
		return ErrShardConsensusMetadataNil
	}
	normalized, err := normalizeShardConsensusMetadata(metadata)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if normalized.Shard != store.shard {
		return fmt.Errorf("%w: shard does not match store", ErrShardConsensusMetadataInvalid)
	}
	store.current = normalized
	return nil
}

// MarshalBinary encodes the current metadata as a compact deterministic HCM1
// record suitable for a durable checkpoint or wire transfer.
func (store *ShardConsensusMetadataStore) MarshalBinary() ([]byte, error) {
	if store == nil {
		return nil, ErrShardConsensusMetadataNil
	}
	return store.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and atomically restores metadata in the store.
func (store *ShardConsensusMetadataStore) UnmarshalBinary(data []byte) error {
	if store == nil {
		return ErrShardConsensusMetadataNil
	}
	metadata, err := UnmarshalShardConsensusMetadata(data)
	if err != nil {
		return err
	}
	return store.Restore(metadata)
}

// MarshalBinary encodes a metadata record with bounded length-prefixed
// identities and six big-endian uint64 counters. The format avoids JSON field
// overhead while retaining deterministic ordering and strict size limits.
func (metadata ShardConsensusMetadata) MarshalBinary() ([]byte, error) {
	normalized, err := normalizeShardConsensusMetadata(metadata)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, shardConsensusMetadataFixedBytes+(2+len(normalized.Shard))+(2+len(normalized.Owner))+(2+len(normalized.VotedFor)))
	encoded = append(encoded, 'H', 'C', 'M', '1', shardConsensusMetadataVersion)
	encoded = appendShardConsensusMetadataString(encoded, normalized.Shard)
	encoded = appendShardConsensusMetadataString(encoded, normalized.Owner)
	encoded = appendShardConsensusMetadataString(encoded, normalized.VotedFor)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.FencingToken)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.Term)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.CommitIndex)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.AppliedIndex)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.Frontier)
	encoded = appendShardConsensusMetadataUint64(encoded, normalized.ConfigurationGeneration)
	return encoded, nil
}

// UnmarshalShardConsensusMetadata decodes and validates one strict HCM1
// record, rejecting truncation, invalid identities, and trailing bytes.
func UnmarshalShardConsensusMetadata(encoded []byte) (ShardConsensusMetadata, error) {
	if len(encoded) < 5+2*3+8*6 || len(encoded) > maxShardConsensusMetadataSnapshotBytes {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	if string(encoded[:4]) != "HCM1" || encoded[4] != shardConsensusMetadataVersion {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	offset := 5
	shard, ok := readShardConsensusMetadataString(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	owner, ok := readShardConsensusMetadataString(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	votedFor, ok := readShardConsensusMetadataString(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	fencingToken, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	term, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	commitIndex, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	appliedIndex, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	frontier, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	configurationGeneration, ok := readShardConsensusMetadataUint64(encoded, &offset)
	if !ok || offset != len(encoded) {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	metadata, err := normalizeShardConsensusMetadata(ShardConsensusMetadata{
		Shard:                   shard,
		Owner:                   owner,
		FencingToken:            fencingToken,
		Term:                    term,
		VotedFor:                votedFor,
		CommitIndex:             commitIndex,
		AppliedIndex:            appliedIndex,
		Frontier:                frontier,
		ConfigurationGeneration: configurationGeneration,
	})
	if err != nil {
		return ShardConsensusMetadata{}, ErrShardConsensusMetadataSnapshotInvalid
	}
	return metadata, nil
}

func normalizeShardConsensusMetadata(metadata ShardConsensusMetadata) (ShardConsensusMetadata, error) {
	shard, err := normalizeShardConsensusMetadataIdentity(metadata.Shard, true)
	if err != nil {
		return ShardConsensusMetadata{}, err
	}
	owner, err := normalizeShardConsensusMetadataIdentity(metadata.Owner, false)
	if err != nil {
		return ShardConsensusMetadata{}, err
	}
	votedFor, err := normalizeShardConsensusMetadataIdentity(metadata.VotedFor, false)
	if err != nil {
		return ShardConsensusMetadata{}, err
	}
	if owner == "" && metadata.FencingToken != 0 {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: fencing token requires an owner", ErrShardConsensusMetadataInvalid)
	}
	if owner != "" && metadata.FencingToken == 0 {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: owner requires a fencing token", ErrShardConsensusMetadataInvalid)
	}
	if metadata.Term == 0 && votedFor != "" {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: vote requires a non-zero term", ErrShardConsensusMetadataInvalid)
	}
	if metadata.AppliedIndex > metadata.CommitIndex {
		return ShardConsensusMetadata{}, fmt.Errorf("%w: applied index exceeds commit index", ErrShardConsensusMetadataInvalid)
	}
	metadata.Shard = shard
	metadata.Owner = owner
	metadata.VotedFor = votedFor
	return metadata, nil
}

func normalizeShardConsensusMetadataIdentity(value string, required bool) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		if required {
			return "", ErrShardConsensusMetadataInvalid
		}
		return "", nil
	}
	if len(trimmed) > MaxShardConsensusMetadataIdentityBytes || !utf8.ValidString(trimmed) || strings.IndexByte(trimmed, 0) >= 0 {
		return "", ErrShardConsensusMetadataInvalid
	}
	return trimmed, nil
}

func appendShardConsensusMetadataString(encoded []byte, value string) []byte {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	encoded = append(encoded, length[:]...)
	return append(encoded, value...)
}

func appendShardConsensusMetadataUint64(encoded []byte, value uint64) []byte {
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], value)
	return append(encoded, number[:]...)
}

func readShardConsensusMetadataString(encoded []byte, offset *int) (string, bool) {
	if *offset < 0 || len(encoded)-*offset < 2 {
		return "", false
	}
	length := int(binary.BigEndian.Uint16(encoded[*offset : *offset+2]))
	*offset += 2
	if length > MaxShardConsensusMetadataIdentityBytes || length > len(encoded)-*offset {
		return "", false
	}
	value := string(encoded[*offset : *offset+length])
	*offset += length
	if value == "" {
		return "", true
	}
	if strings.TrimSpace(value) != value || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return "", false
	}
	return value, true
}

func readShardConsensusMetadataUint64(encoded []byte, offset *int) (uint64, bool) {
	if *offset < 0 || len(encoded)-*offset < 8 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(encoded[*offset : *offset+8])
	*offset += 8
	return value, true
}
