package hatSql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLShardConsensusMetadataShardCount bounds unrelated metadata
	// callers' configured shard layout. The registry uses one short critical
	// section because generation assignment must be atomic.
	DefaultSQLShardConsensusMetadataShardCount = 16
	// DefaultSQLShardConsensusMetadataMaxShards bounds retained metadata rows.
	DefaultSQLShardConsensusMetadataMaxShards = 65536
	// DefaultSQLShardConsensusMetadataMaxShardIDBytes bounds shard IDs.
	DefaultSQLShardConsensusMetadataMaxShardIDBytes = 256
	// DefaultSQLShardConsensusMetadataMaxOwnerBytes bounds owner IDs.
	DefaultSQLShardConsensusMetadataMaxOwnerBytes = 128
	maxSQLShardConsensusMetadataShardCount        = 1024
	maxSQLShardConsensusMetadataIDBytes           = 4096
	maxSQLShardConsensusMetadataOwnerBytes        = 4096
	maxSQLShardConsensusMetadataBytes             = 16 << 20
	maxSQLShardConsensusMetadataEntries           = DefaultSQLShardConsensusMetadataMaxShards
)

var (
	// ErrSQLShardConsensusMetadataRegistryNil reports a nil registry receiver.
	ErrSQLShardConsensusMetadataRegistryNil = errors.New("SQL shard consensus metadata registry is nil")
	// ErrSQLShardConsensusContextNil reports a nil context.
	ErrSQLShardConsensusContextNil = errors.New("SQL shard consensus metadata context is nil")
	// ErrSQLShardConsensusCheckpointStoreRequired reports a missing durable store.
	ErrSQLShardConsensusCheckpointStoreRequired = errors.New("SQL shard consensus metadata checkpoint store is required")
	// ErrSQLShardConsensusShardRequired reports a missing shard ID.
	ErrSQLShardConsensusShardRequired = errors.New("SQL shard consensus metadata shard ID is required")
	// ErrSQLShardConsensusShardTooLarge reports an oversized shard ID.
	ErrSQLShardConsensusShardTooLarge = errors.New("SQL shard consensus metadata shard ID is too large")
	// ErrSQLShardConsensusOwnerRequired reports a missing owner ID.
	ErrSQLShardConsensusOwnerRequired = errors.New("SQL shard consensus metadata owner is required")
	// ErrSQLShardConsensusOwnerTooLarge reports an oversized owner ID.
	ErrSQLShardConsensusOwnerTooLarge = errors.New("SQL shard consensus metadata owner is too large")
	// ErrSQLShardConsensusFencingRequired reports a zero fencing token.
	ErrSQLShardConsensusFencingRequired = errors.New("SQL shard consensus metadata fencing token is required")
	// ErrSQLShardConsensusCapacity reports a full metadata registry.
	ErrSQLShardConsensusCapacity = errors.New("SQL shard consensus metadata capacity exceeded")
	// ErrSQLShardConsensusShardNotFound reports an update for an unknown shard.
	ErrSQLShardConsensusShardNotFound = errors.New("SQL shard consensus metadata shard is not found")
	// ErrSQLShardConsensusStaleOwner reports a token or owner older than the
	// current durable record.
	ErrSQLShardConsensusStaleOwner = errors.New("SQL shard consensus metadata owner is stale")
	// ErrSQLShardConsensusFrontierRegression reports a lower frontier.
	ErrSQLShardConsensusFrontierRegression = errors.New("SQL shard consensus frontier regressed")
	// ErrSQLShardConsensusGenerationExhausted reports an unsafe generation wrap.
	ErrSQLShardConsensusGenerationExhausted = errors.New("SQL shard consensus metadata generation exhausted")
	// ErrSQLShardConsensusTermExhausted reports an unsafe term wrap.
	ErrSQLShardConsensusTermExhausted = errors.New("SQL shard consensus metadata term exhausted")
	// ErrSQLShardConsensusSnapshotInvalid reports malformed or unsafe checkpoint data.
	ErrSQLShardConsensusSnapshotInvalid = errors.New("SQL shard consensus metadata snapshot is invalid")
)

// SQLShardConsensusMetadataRegistryOptions bounds a metadata registry. Zero
// values use the documented defaults.
type SQLShardConsensusMetadataRegistryOptions struct {
	ShardCount      int
	MaxShards       int
	MaxShardIDBytes int
	MaxOwnerBytes   int
}

// SQLShardConsensusMetadata records durable ownership and frontier state for
// one shard. FencingToken comes from SQLShardLeaseRegistry; Term advances on
// ownership changes, while Generation advances on every accepted mutation.
type SQLShardConsensusMetadata struct {
	ShardID          string `json:"shard_id"`
	Term             uint64 `json:"term"`
	Owner            string `json:"owner"`
	FencingToken     uint64 `json:"fencing_token"`
	Frontier         uint64 `json:"frontier"`
	FrontierObserved bool   `json:"frontier_observed"`
	Revision         uint64 `json:"revision"`
	Generation       uint64 `json:"generation"`
}

// SQLShardConsensusMetadataSnapshot is a deterministic, independently owned
// checkpoint. Generation is the compare-and-swap value for durable stores.
type SQLShardConsensusMetadataSnapshot struct {
	Generation uint64                      `json:"generation"`
	Shards     []SQLShardConsensusMetadata `json:"shards"`
}

// SQLShardConsensusMetadataCheckpointStore atomically loads and conditionally
// commits the complete metadata snapshot. Implementations should commit only
// when expectedGeneration matches their current durable generation.
type SQLShardConsensusMetadataCheckpointStore interface {
	Load(context.Context) (SQLShardConsensusMetadataSnapshot, bool, error)
	Commit(context.Context, uint64, SQLShardConsensusMetadataSnapshot) (bool, error)
}

// SQLShardConsensusMetadataRegistryStats reports bounded metadata state.
type SQLShardConsensusMetadataRegistryStats struct {
	Active          int
	Shards          int
	MaxShards       int
	Generation      uint64
	MaxShardIDBytes int
	MaxOwnerBytes   int
}

// SQLShardConsensusMetadataRegistry owns the in-process copy of durable shard
// metadata. Durable publication is explicit through CommitTo; this registry is
// not a network consensus or leader-election implementation.
type SQLShardConsensusMetadataRegistry struct {
	mu              sync.RWMutex
	metadata        map[string]SQLShardConsensusMetadata
	shardCount      int
	maxShards       int
	maxShardIDBytes int
	maxOwnerBytes   int
	generation      uint64
}

// NewSQLShardConsensusMetadataRegistry creates a bounded metadata registry.
func NewSQLShardConsensusMetadataRegistry(options SQLShardConsensusMetadataRegistryOptions) (*SQLShardConsensusMetadataRegistry, error) {
	shardCount := options.ShardCount
	if shardCount <= 0 {
		shardCount = DefaultSQLShardConsensusMetadataShardCount
	}
	if shardCount > maxSQLShardConsensusMetadataShardCount {
		shardCount = maxSQLShardConsensusMetadataShardCount
	}
	maxShards := options.MaxShards
	if maxShards <= 0 {
		maxShards = DefaultSQLShardConsensusMetadataMaxShards
	}
	if maxShards > maxSQLShardConsensusMetadataEntries {
		maxShards = maxSQLShardConsensusMetadataEntries
	}
	maxShardIDBytes := options.MaxShardIDBytes
	if maxShardIDBytes <= 0 {
		maxShardIDBytes = DefaultSQLShardConsensusMetadataMaxShardIDBytes
	}
	if maxShardIDBytes > maxSQLShardConsensusMetadataIDBytes {
		maxShardIDBytes = maxSQLShardConsensusMetadataIDBytes
	}
	maxOwnerBytes := options.MaxOwnerBytes
	if maxOwnerBytes <= 0 {
		maxOwnerBytes = DefaultSQLShardConsensusMetadataMaxOwnerBytes
	}
	if maxOwnerBytes > maxSQLShardConsensusMetadataOwnerBytes {
		maxOwnerBytes = maxSQLShardConsensusMetadataOwnerBytes
	}
	return &SQLShardConsensusMetadataRegistry{
		metadata:        make(map[string]SQLShardConsensusMetadata),
		shardCount:      shardCount,
		maxShards:       maxShards,
		maxShardIDBytes: maxShardIDBytes,
		maxOwnerBytes:   maxOwnerBytes,
	}, nil
}

// Claim records ownership for a shard using a lease fencing token. A new
// owner must present a strictly larger token; the existing frontier survives a
// handoff and the term increments.
func (registry *SQLShardConsensusMetadataRegistry) Claim(shardID, owner string, fencingToken uint64) (SQLShardConsensusMetadata, error) {
	shardID = strings.TrimSpace(shardID)
	owner = strings.TrimSpace(owner)
	if err := registry.validateIdentity(shardID, owner, fencingToken); err != nil {
		return SQLShardConsensusMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, found := registry.metadata[shardID]
	if found {
		if current.Owner == owner && current.FencingToken == fencingToken {
			return current, nil
		}
		if fencingToken <= current.FencingToken {
			return SQLShardConsensusMetadata{}, ErrSQLShardConsensusStaleOwner
		}
		if current.Term == ^uint64(0) || current.Revision == ^uint64(0) {
			return SQLShardConsensusMetadata{}, ErrSQLShardConsensusTermExhausted
		}
		current.Term++
		current.Revision++
	} else {
		if len(registry.metadata) >= registry.maxShards {
			return SQLShardConsensusMetadata{}, ErrSQLShardConsensusCapacity
		}
		current = SQLShardConsensusMetadata{ShardID: shardID, Term: 1, Revision: 1}
	}
	generation, err := registry.nextGenerationLocked()
	if err != nil {
		return SQLShardConsensusMetadata{}, err
	}
	current.Owner = owner
	current.FencingToken = fencingToken
	current.Generation = generation
	registry.metadata[shardID] = current
	return current, nil
}

// AdvanceFrontier records a monotone frontier under the exact current owner
// and fencing token. Equal updates are idempotent; the first observed zero is
// still meaningful because FrontierObserved distinguishes it from no update.
func (registry *SQLShardConsensusMetadataRegistry) AdvanceFrontier(shardID, owner string, fencingToken, frontier uint64) (SQLShardConsensusMetadata, error) {
	shardID = strings.TrimSpace(shardID)
	owner = strings.TrimSpace(owner)
	if err := registry.validateIdentity(shardID, owner, fencingToken); err != nil {
		return SQLShardConsensusMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, found := registry.metadata[shardID]
	if !found {
		return SQLShardConsensusMetadata{}, ErrSQLShardConsensusShardNotFound
	}
	if current.Owner != owner || current.FencingToken != fencingToken {
		return SQLShardConsensusMetadata{}, ErrSQLShardConsensusStaleOwner
	}
	if current.FrontierObserved && frontier < current.Frontier {
		return SQLShardConsensusMetadata{}, ErrSQLShardConsensusFrontierRegression
	}
	if current.FrontierObserved && frontier == current.Frontier {
		return current, nil
	}
	if current.Revision == ^uint64(0) {
		return SQLShardConsensusMetadata{}, ErrSQLShardConsensusTermExhausted
	}
	generation, err := registry.nextGenerationLocked()
	if err != nil {
		return SQLShardConsensusMetadata{}, err
	}
	current.Frontier = frontier
	current.FrontierObserved = true
	current.Revision++
	current.Generation = generation
	registry.metadata[shardID] = current
	return current, nil
}

// Get returns an independently owned current record.
func (registry *SQLShardConsensusMetadataRegistry) Get(shardID string) (SQLShardConsensusMetadata, bool) {
	if registry == nil || shardID == "" || len(shardID) > registry.maxShardIDBytes {
		return SQLShardConsensusMetadata{}, false
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	metadata, found := registry.metadata[strings.TrimSpace(shardID)]
	return metadata, found
}

// Snapshot returns metadata in deterministic shard-ID order.
func (registry *SQLShardConsensusMetadataRegistry) Snapshot() SQLShardConsensusMetadataSnapshot {
	if registry == nil {
		return SQLShardConsensusMetadataSnapshot{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	shards := make([]SQLShardConsensusMetadata, 0, len(registry.metadata))
	for _, metadata := range registry.metadata {
		shards = append(shards, metadata)
	}
	sort.Slice(shards, func(left, right int) bool { return shards[left].ShardID < shards[right].ShardID })
	return SQLShardConsensusMetadataSnapshot{Generation: registry.generation, Shards: shards}
}

// Restore atomically replaces all metadata with a validated snapshot.
func (registry *SQLShardConsensusMetadataRegistry) Restore(snapshot SQLShardConsensusMetadataSnapshot) error {
	if registry == nil {
		return ErrSQLShardConsensusMetadataRegistryNil
	}
	validated, err := registry.validateSnapshot(snapshot)
	if err != nil {
		return err
	}
	replacement := make(map[string]SQLShardConsensusMetadata, len(validated.Shards))
	for _, metadata := range validated.Shards {
		replacement[metadata.ShardID] = metadata
	}
	registry.mu.Lock()
	registry.metadata = replacement
	registry.generation = validated.Generation
	registry.mu.Unlock()
	return nil
}

// CommitTo conditionally publishes the current snapshot to a durable store.
// The store decides whether expectedGeneration still matches its durable copy.
func (registry *SQLShardConsensusMetadataRegistry) CommitTo(ctx context.Context, store SQLShardConsensusMetadataCheckpointStore, expectedGeneration uint64) (bool, error) {
	if registry == nil {
		return false, ErrSQLShardConsensusMetadataRegistryNil
	}
	if ctx == nil {
		return false, ErrSQLShardConsensusContextNil
	}
	if store == nil {
		return false, ErrSQLShardConsensusCheckpointStoreRequired
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return store.Commit(ctx, expectedGeneration, registry.Snapshot())
}

// RestoreFrom loads and applies one durable snapshot. A missing store record
// returns found=false without changing the registry.
func (registry *SQLShardConsensusMetadataRegistry) RestoreFrom(ctx context.Context, store SQLShardConsensusMetadataCheckpointStore) (bool, error) {
	if registry == nil {
		return false, ErrSQLShardConsensusMetadataRegistryNil
	}
	if ctx == nil {
		return false, ErrSQLShardConsensusContextNil
	}
	if store == nil {
		return false, ErrSQLShardConsensusCheckpointStoreRequired
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	snapshot, found, err := store.Load(ctx)
	if err != nil || !found {
		return found, err
	}
	if err := registry.Restore(snapshot); err != nil {
		return false, err
	}
	return true, nil
}

// Stats reports active metadata and registry bounds.
func (registry *SQLShardConsensusMetadataRegistry) Stats() SQLShardConsensusMetadataRegistryStats {
	if registry == nil {
		return SQLShardConsensusMetadataRegistryStats{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return SQLShardConsensusMetadataRegistryStats{
		Active:          len(registry.metadata),
		Shards:          registry.shardCount,
		MaxShards:       registry.maxShards,
		Generation:      registry.generation,
		MaxShardIDBytes: registry.maxShardIDBytes,
		MaxOwnerBytes:   registry.maxOwnerBytes,
	}
}

// MarshalBinary encodes a bounded deterministic checkpoint with CRC32
// corruption detection and no JSON reflection metadata.
func (registry *SQLShardConsensusMetadataRegistry) MarshalBinary() ([]byte, error) {
	if registry == nil {
		return nil, ErrSQLShardConsensusMetadataRegistryNil
	}
	return marshalSQLShardConsensusMetadataSnapshot(registry.Snapshot())
}

// UnmarshalSQLShardConsensusMetadataSnapshot decodes a checkpoint produced by
// MarshalBinary. Restore applies caller-specific bounds.
func UnmarshalSQLShardConsensusMetadataSnapshot(data []byte) (SQLShardConsensusMetadataSnapshot, error) {
	if len(data) < 9 || len(data) > maxSQLShardConsensusMetadataBytes || !bytes.Equal(data[:4], []byte("HSM1")) || data[4] != 1 {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	payloadLength := len(data) - 4
	if crc32.ChecksumIEEE(data[:payloadLength]) != binary.LittleEndian.Uint32(data[payloadLength:]) {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	reader := sqlShardConsensusMetadataReader{data: data[:payloadLength], offset: 5}
	generation, ok := reader.uvarint()
	if !ok {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	count, ok := reader.uvarint()
	if !ok || count > maxSQLShardConsensusMetadataEntries {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	snapshot := SQLShardConsensusMetadataSnapshot{
		Generation: generation,
		Shards:     make([]SQLShardConsensusMetadata, 0, int(count)),
	}
	for index := uint64(0); index < count; index++ {
		shardID, ok := reader.string(maxSQLShardConsensusMetadataIDBytes)
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		owner, ok := reader.string(maxSQLShardConsensusMetadataOwnerBytes)
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		term, ok := reader.uvarint()
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		fencingToken, ok := reader.uvarint()
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		frontier, ok := reader.uvarint()
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		observed, ok := reader.byte()
		if !ok || observed > 1 {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		revision, ok := reader.uvarint()
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		metadataGeneration, ok := reader.uvarint()
		if !ok {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		snapshot.Shards = append(snapshot.Shards, SQLShardConsensusMetadata{
			ShardID:          shardID,
			Term:             term,
			Owner:            owner,
			FencingToken:     fencingToken,
			Frontier:         frontier,
			FrontierObserved: observed == 1,
			Revision:         revision,
			Generation:       metadataGeneration,
		})
	}
	if !reader.done() {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	return snapshot, nil
}

func (registry *SQLShardConsensusMetadataRegistry) validateIdentity(shardID, owner string, fencingToken uint64) error {
	if registry == nil {
		return ErrSQLShardConsensusMetadataRegistryNil
	}
	shardID = strings.TrimSpace(shardID)
	if shardID == "" {
		return ErrSQLShardConsensusShardRequired
	}
	if len(shardID) > registry.maxShardIDBytes {
		return ErrSQLShardConsensusShardTooLarge
	}
	if strings.TrimSpace(owner) == "" {
		return ErrSQLShardConsensusOwnerRequired
	}
	if len(owner) > registry.maxOwnerBytes {
		return ErrSQLShardConsensusOwnerTooLarge
	}
	if fencingToken == 0 {
		return ErrSQLShardConsensusFencingRequired
	}
	return nil
}

func (registry *SQLShardConsensusMetadataRegistry) validateSnapshot(snapshot SQLShardConsensusMetadataSnapshot) (SQLShardConsensusMetadataSnapshot, error) {
	if len(snapshot.Shards) > registry.maxShards || len(snapshot.Shards) > maxSQLShardConsensusMetadataEntries {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	if snapshot.Generation == 0 && len(snapshot.Shards) > 0 {
		return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
	}
	validated := SQLShardConsensusMetadataSnapshot{
		Generation: snapshot.Generation,
		Shards:     append([]SQLShardConsensusMetadata(nil), snapshot.Shards...),
	}
	for index := range validated.Shards {
		validated.Shards[index].ShardID = strings.TrimSpace(validated.Shards[index].ShardID)
		validated.Shards[index].Owner = strings.TrimSpace(validated.Shards[index].Owner)
	}
	sort.Slice(validated.Shards, func(left, right int) bool { return validated.Shards[left].ShardID < validated.Shards[right].ShardID })
	for index, metadata := range validated.Shards {
		if err := registry.validateIdentity(metadata.ShardID, metadata.Owner, metadata.FencingToken); err != nil {
			return SQLShardConsensusMetadataSnapshot{}, err
		}
		if metadata.Term == 0 || metadata.Revision == 0 || metadata.Generation == 0 || metadata.Generation > validated.Generation {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
		if index > 0 && validated.Shards[index-1].ShardID == metadata.ShardID {
			return SQLShardConsensusMetadataSnapshot{}, ErrSQLShardConsensusSnapshotInvalid
		}
	}
	return validated, nil
}

func (registry *SQLShardConsensusMetadataRegistry) nextGenerationLocked() (uint64, error) {
	if registry.generation == ^uint64(0) {
		return 0, ErrSQLShardConsensusGenerationExhausted
	}
	registry.generation++
	return registry.generation, nil
}

func marshalSQLShardConsensusMetadataSnapshot(snapshot SQLShardConsensusMetadataSnapshot) ([]byte, error) {
	if len(snapshot.Shards) > maxSQLShardConsensusMetadataEntries || snapshot.Generation == 0 && len(snapshot.Shards) > 0 {
		return nil, ErrSQLShardConsensusSnapshotInvalid
	}
	data := make([]byte, 0, 24+len(snapshot.Shards)*80)
	data = append(data, 'H', 'S', 'M', '1', 1)
	data = appendSQLShardConsensusMetadataUvarint(data, snapshot.Generation)
	data = appendSQLShardConsensusMetadataUvarint(data, uint64(len(snapshot.Shards)))
	for index, metadata := range snapshot.Shards {
		if index > 0 && snapshot.Shards[index-1].ShardID >= metadata.ShardID {
			return nil, ErrSQLShardConsensusSnapshotInvalid
		}
		if metadata.ShardID == "" || len(metadata.ShardID) > maxSQLShardConsensusMetadataIDBytes || metadata.Owner == "" || len(metadata.Owner) > maxSQLShardConsensusMetadataOwnerBytes || metadata.Term == 0 || metadata.FencingToken == 0 || metadata.Revision == 0 || metadata.Generation == 0 || metadata.Generation > snapshot.Generation {
			return nil, ErrSQLShardConsensusSnapshotInvalid
		}
		data = appendSQLShardConsensusMetadataString(data, metadata.ShardID)
		data = appendSQLShardConsensusMetadataString(data, metadata.Owner)
		data = appendSQLShardConsensusMetadataUvarint(data, metadata.Term)
		data = appendSQLShardConsensusMetadataUvarint(data, metadata.FencingToken)
		data = appendSQLShardConsensusMetadataUvarint(data, metadata.Frontier)
		if metadata.FrontierObserved {
			data = append(data, 1)
		} else {
			data = append(data, 0)
		}
		data = appendSQLShardConsensusMetadataUvarint(data, metadata.Revision)
		data = appendSQLShardConsensusMetadataUvarint(data, metadata.Generation)
		if len(data)+4 > maxSQLShardConsensusMetadataBytes {
			return nil, ErrSQLShardConsensusSnapshotInvalid
		}
	}
	checksum := crc32.ChecksumIEEE(data)
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], checksum)
	data = append(data, encoded[:]...)
	return data, nil
}

func appendSQLShardConsensusMetadataString(data []byte, value string) []byte {
	data = appendSQLShardConsensusMetadataUvarint(data, uint64(len(value)))
	return append(data, value...)
}

func appendSQLShardConsensusMetadataUvarint(data []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(data, encoded[:length]...)
}

type sqlShardConsensusMetadataReader struct {
	data   []byte
	offset int
}

func (reader *sqlShardConsensusMetadataReader) uvarint() (uint64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, length := binary.Uvarint(reader.data[reader.offset:])
	if length <= 0 {
		return 0, false
	}
	reader.offset += length
	return value, true
}

func (reader *sqlShardConsensusMetadataReader) byte() (byte, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, true
}

func (reader *sqlShardConsensusMetadataReader) string(maxBytes int) (string, bool) {
	length, ok := reader.uvarint()
	if !ok || length == 0 || length > uint64(maxBytes) || length > uint64(len(reader.data)-reader.offset) {
		return "", false
	}
	start := reader.offset
	reader.offset += int(length)
	return string(reader.data[start:reader.offset]), true
}

func (reader *sqlShardConsensusMetadataReader) done() bool {
	return reader.offset == len(reader.data)
}
