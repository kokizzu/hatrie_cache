package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultSQLShardLeaseShardCount bounds unrelated shard contention without
	// allocating a large per-shard map.
	DefaultSQLShardLeaseShardCount = 16
	// DefaultSQLShardLeaseMaxShards bounds retained ownership records.
	DefaultSQLShardLeaseMaxShards = 65536
	// DefaultSQLShardLeaseDuration is used when no lease duration is supplied.
	DefaultSQLShardLeaseDuration = 30 * time.Second
	// DefaultSQLShardLeaseMaxShardIDBytes bounds caller-controlled shard IDs.
	DefaultSQLShardLeaseMaxShardIDBytes = 256
	// DefaultSQLShardLeaseMaxOwnerBytes bounds caller-controlled owner IDs.
	DefaultSQLShardLeaseMaxOwnerBytes = 128
	maxSQLShardLeaseShardCount        = 1024
	maxSQLShardLeaseIDBytes           = 4096
	maxSQLShardLeaseMaxBytes          = 16 << 20
	maxSQLShardLeaseOwnerBytes        = 4096
	maxSQLShardLeaseSnapshotEntries   = DefaultSQLShardLeaseMaxShards
)

var (
	// ErrSQLShardLeaseRegistryNil reports a method call on a nil registry.
	ErrSQLShardLeaseRegistryNil = errors.New("SQL shard lease registry is nil")
	// ErrSQLShardLeaseShardRequired reports a missing shard ID.
	ErrSQLShardLeaseShardRequired = errors.New("SQL shard lease shard ID is required")
	// ErrSQLShardLeaseShardTooLarge reports a shard ID beyond the configured bound.
	ErrSQLShardLeaseShardTooLarge = errors.New("SQL shard lease shard ID is too large")
	// ErrSQLShardLeaseOwnerRequired reports a missing owner ID.
	ErrSQLShardLeaseOwnerRequired = errors.New("SQL shard lease owner is required")
	// ErrSQLShardLeaseOwnerTooLarge reports an owner ID beyond the configured bound.
	ErrSQLShardLeaseOwnerTooLarge = errors.New("SQL shard lease owner is too large")
	// ErrSQLShardLeaseHeld reports an unexpired lease owned by another fencing
	// token. Callers must wait for expiry or renew their own lease.
	ErrSQLShardLeaseHeld = errors.New("SQL shard lease is held")
	// ErrSQLShardLeaseStale reports a release or renew from an older owner.
	ErrSQLShardLeaseStale = errors.New("SQL shard lease is stale")
	// ErrSQLShardLeaseCapacity reports that the registry reached MaxShards.
	ErrSQLShardLeaseCapacity = errors.New("SQL shard lease capacity exceeded")
	// ErrSQLShardLeaseFencingExhausted reports that the fencing sequence cannot
	// safely advance without wrapping.
	ErrSQLShardLeaseFencingExhausted = errors.New("SQL shard lease fencing sequence exhausted")
	// ErrSQLShardLeaseSnapshotInvalid reports malformed or unsafe checkpoint data.
	ErrSQLShardLeaseSnapshotInvalid = errors.New("SQL shard lease snapshot is invalid")
)

// SQLShardLeaseRegistryOptions bounds a shard lease registry. Zero values use
// the documented defaults. Now is injectable for deterministic recovery tests.
type SQLShardLeaseRegistryOptions struct {
	ShardCount      int
	MaxShards       int
	LeaseDuration   time.Duration
	MaxShardIDBytes int
	MaxOwnerBytes   int
	Now             func() time.Time
}

// SQLShardLease identifies the exact ownership generation for one shard.
// FencingToken must be carried with work so a stale owner can be rejected by
// the downstream state store.
type SQLShardLease struct {
	ShardID      string    `json:"shard_id"`
	Owner        string    `json:"owner"`
	FencingToken uint64    `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// SQLShardLeaseSnapshot is a deterministic, independently owned checkpoint.
// NextFencingToken is retained even when expired leases are omitted so a
// restored registry never reuses an older fencing token.
type SQLShardLeaseSnapshot struct {
	NextFencingToken uint64          `json:"next_fencing_token"`
	Leases           []SQLShardLease `json:"leases"`
}

// SQLShardLeaseRegistryStats is a point-in-time capacity summary.
type SQLShardLeaseRegistryStats struct {
	Active           int
	Shards           int
	MaxShards        int
	MaxShardIDBytes  int
	MaxOwnerBytes    int
	LeaseDuration    time.Duration
	NextFencingToken uint64
}

type sqlShardLeaseShard struct {
	mu     sync.Mutex
	leases map[string]SQLShardLease
}

// SQLShardLeaseRegistry is an opt-in, bounded ownership registry. It does not
// write files or start renewal goroutines; callers persist MarshalBinary's
// checkpoint atomically and restore it before starting state workers.
type SQLShardLeaseRegistry struct {
	shards          []sqlShardLeaseShard
	maxShards       int
	maxShardIDBytes int
	maxOwnerBytes   int
	leaseDuration   time.Duration
	now             func() time.Time
	active          atomic.Int64
	nextToken       atomic.Uint64
}

// NewSQLShardLeaseRegistry creates a bounded shard lease registry.
func NewSQLShardLeaseRegistry(options SQLShardLeaseRegistryOptions) (*SQLShardLeaseRegistry, error) {
	shardCount := options.ShardCount
	if shardCount <= 0 {
		shardCount = DefaultSQLShardLeaseShardCount
	}
	if shardCount > maxSQLShardLeaseShardCount {
		shardCount = maxSQLShardLeaseShardCount
	}
	maxShards := options.MaxShards
	if maxShards <= 0 {
		maxShards = DefaultSQLShardLeaseMaxShards
	}
	if maxShards > maxSQLShardLeaseSnapshotEntries {
		maxShards = maxSQLShardLeaseSnapshotEntries
	}
	maxShardIDBytes := options.MaxShardIDBytes
	if maxShardIDBytes <= 0 {
		maxShardIDBytes = DefaultSQLShardLeaseMaxShardIDBytes
	}
	if maxShardIDBytes > maxSQLShardLeaseIDBytes {
		maxShardIDBytes = maxSQLShardLeaseIDBytes
	}
	maxOwnerBytes := options.MaxOwnerBytes
	if maxOwnerBytes <= 0 {
		maxOwnerBytes = DefaultSQLShardLeaseMaxOwnerBytes
	}
	if maxOwnerBytes > maxSQLShardLeaseOwnerBytes {
		maxOwnerBytes = maxSQLShardLeaseOwnerBytes
	}
	leaseDuration := options.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = DefaultSQLShardLeaseDuration
	}
	if leaseDuration <= 0 || maxShards <= 0 || maxShardIDBytes <= 0 || maxOwnerBytes <= 0 {
		return nil, ErrSQLShardLeaseSnapshotInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	registry := &SQLShardLeaseRegistry{
		shards:          make([]sqlShardLeaseShard, shardCount),
		maxShards:       maxShards,
		maxShardIDBytes: maxShardIDBytes,
		maxOwnerBytes:   maxOwnerBytes,
		leaseDuration:   leaseDuration,
		now:             now,
	}
	for index := range registry.shards {
		registry.shards[index].leases = make(map[string]SQLShardLease)
	}
	registry.nextToken.Store(1)
	return registry, nil
}

// Acquire obtains an unexpired shard lease and assigns a new fencing token.
// An existing unexpired lease always blocks acquisition, including when the
// requested owner ID is equal; renewals must use the returned lease value.
func (registry *SQLShardLeaseRegistry) Acquire(shardID, owner string) (SQLShardLease, error) {
	if err := registry.validateIdentity(shardID, owner); err != nil {
		return SQLShardLease{}, err
	}
	now := registry.now().UTC()
	shard := registry.shard(shardID)
	shard.mu.Lock()
	if current, ok := shard.leases[shardID]; ok {
		if current.ExpiresAt.After(now) {
			shard.mu.Unlock()
			return SQLShardLease{}, ErrSQLShardLeaseHeld
		}
		delete(shard.leases, shardID)
		registry.active.Add(-1)
	}
	if registry.active.Load() >= int64(registry.maxShards) {
		shard.mu.Unlock()
		registry.pruneExpiredLocked(now)
		shard.mu.Lock()
		if current, ok := shard.leases[shardID]; ok {
			if current.ExpiresAt.After(now) {
				shard.mu.Unlock()
				return SQLShardLease{}, ErrSQLShardLeaseHeld
			}
			delete(shard.leases, shardID)
			registry.active.Add(-1)
		}
		if registry.active.Load() >= int64(registry.maxShards) {
			shard.mu.Unlock()
			return SQLShardLease{}, ErrSQLShardLeaseCapacity
		}
	}
	token, err := registry.allocateFencingToken()
	if err != nil {
		shard.mu.Unlock()
		return SQLShardLease{}, err
	}
	lease := SQLShardLease{
		ShardID:      shardID,
		Owner:        owner,
		FencingToken: token,
		ExpiresAt:    now.Add(registry.leaseDuration),
	}
	shard.leases[shardID] = lease
	registry.active.Add(1)
	shard.mu.Unlock()
	return lease, nil
}

// Renew extends an unexpired lease without changing its fencing token. A
// lease that already expired or was replaced by another owner is stale.
func (registry *SQLShardLeaseRegistry) Renew(lease SQLShardLease) (SQLShardLease, error) {
	if err := registry.validateLease(lease); err != nil {
		return SQLShardLease{}, err
	}
	now := registry.now().UTC()
	shard := registry.shard(lease.ShardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	current, ok := shard.leases[lease.ShardID]
	if !ok || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken {
		return SQLShardLease{}, ErrSQLShardLeaseStale
	}
	if !current.ExpiresAt.After(now) {
		delete(shard.leases, lease.ShardID)
		registry.active.Add(-1)
		return SQLShardLease{}, ErrSQLShardLeaseStale
	}
	current.ExpiresAt = now.Add(registry.leaseDuration)
	shard.leases[lease.ShardID] = current
	return current, nil
}

// Release removes an owned lease. A stale owner cannot release a replacement
// lease, which prevents delayed cleanup from deleting a new owner's state.
func (registry *SQLShardLeaseRegistry) Release(lease SQLShardLease) (bool, error) {
	if err := registry.validateLease(lease); err != nil {
		return false, err
	}
	shard := registry.shard(lease.ShardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	current, ok := shard.leases[lease.ShardID]
	if !ok {
		return false, nil
	}
	if current.Owner != lease.Owner || current.FencingToken != lease.FencingToken {
		return false, ErrSQLShardLeaseStale
	}
	delete(shard.leases, lease.ShardID)
	registry.active.Add(-1)
	return true, nil
}

// Get returns the current unexpired lease for a shard. Expired entries are
// removed lazily and are reported as absent.
func (registry *SQLShardLeaseRegistry) Get(shardID string) (SQLShardLease, bool) {
	if registry == nil || shardID == "" || len(shardID) > registry.maxShardIDBytes {
		return SQLShardLease{}, false
	}
	now := registry.now().UTC()
	shard := registry.shard(shardID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	lease, ok := shard.leases[shardID]
	if !ok {
		return SQLShardLease{}, false
	}
	if !lease.ExpiresAt.After(now) {
		delete(shard.leases, shardID)
		registry.active.Add(-1)
		return SQLShardLease{}, false
	}
	return lease, true
}

// Stats reports registry bounds and active lease count without copying lease
// records.
func (registry *SQLShardLeaseRegistry) Stats() SQLShardLeaseRegistryStats {
	if registry == nil {
		return SQLShardLeaseRegistryStats{}
	}
	return SQLShardLeaseRegistryStats{
		Active:           int(registry.active.Load()),
		Shards:           len(registry.shards),
		MaxShards:        registry.maxShards,
		MaxShardIDBytes:  registry.maxShardIDBytes,
		MaxOwnerBytes:    registry.maxOwnerBytes,
		LeaseDuration:    registry.leaseDuration,
		NextFencingToken: registry.nextToken.Load(),
	}
}

// Snapshot returns active leases in shard-ID order. Expired leases are
// omitted, while NextFencingToken preserves the fencing history.
func (registry *SQLShardLeaseRegistry) Snapshot() SQLShardLeaseSnapshot {
	if registry == nil {
		return SQLShardLeaseSnapshot{}
	}
	now := registry.now().UTC()
	registry.lockShards()
	defer registry.unlockShards()
	leases := make([]SQLShardLease, 0, int(registry.active.Load()))
	for index := range registry.shards {
		for shardID, lease := range registry.shards[index].leases {
			if lease.ExpiresAt.After(now) {
				leases = append(leases, lease)
				continue
			}
			delete(registry.shards[index].leases, shardID)
			registry.active.Add(-1)
		}
	}
	sort.Slice(leases, func(left, right int) bool { return leases[left].ShardID < leases[right].ShardID })
	return SQLShardLeaseSnapshot{NextFencingToken: registry.nextToken.Load(), Leases: leases}
}

// Restore atomically replaces the registry with a validated checkpoint. The
// checkpoint's next token is restored even when some lease records are already
// expired at the current clock.
func (registry *SQLShardLeaseRegistry) Restore(snapshot SQLShardLeaseSnapshot) error {
	if registry == nil {
		return ErrSQLShardLeaseRegistryNil
	}
	validated, err := registry.validateSnapshot(snapshot)
	if err != nil {
		return err
	}
	now := registry.now().UTC()
	active := make([]SQLShardLease, 0, len(validated.Leases))
	for _, lease := range validated.Leases {
		if lease.ExpiresAt.After(now) {
			active = append(active, lease)
		}
	}
	registry.lockShards()
	defer registry.unlockShards()
	for index := range registry.shards {
		registry.shards[index].leases = make(map[string]SQLShardLease)
	}
	for _, lease := range active {
		shard := registry.shard(lease.ShardID)
		shard.leases[lease.ShardID] = lease
	}
	registry.active.Store(int64(len(active)))
	registry.nextToken.Store(validated.NextFencingToken)
	return nil
}

// MarshalBinary encodes a bounded, deterministic checkpoint without JSON
// reflection or per-record map metadata.
func (registry *SQLShardLeaseRegistry) MarshalBinary() ([]byte, error) {
	if registry == nil {
		return nil, ErrSQLShardLeaseRegistryNil
	}
	return marshalSQLShardLeaseSnapshot(registry.Snapshot())
}

// UnmarshalSQLShardLeaseSnapshot decodes and validates the bounded binary
// checkpoint produced by MarshalBinary. Restore applies caller-specific bounds.
func UnmarshalSQLShardLeaseSnapshot(data []byte) (SQLShardLeaseSnapshot, error) {
	if len(data) < 9 || len(data) > maxSQLShardLeaseMaxBytes || !bytes.Equal(data[:4], []byte("HSL1")) || data[4] != 1 {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	payloadLength := len(data) - 4
	if crc32.ChecksumIEEE(data[:payloadLength]) != binary.LittleEndian.Uint32(data[payloadLength:]) {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	reader := sqlShardLeaseReader{data: data[:payloadLength], offset: 5}
	nextToken, ok := reader.uvarint()
	if !ok || nextToken == 0 {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	count, ok := reader.uvarint()
	if !ok || count > maxSQLShardLeaseSnapshotEntries {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	snapshot := SQLShardLeaseSnapshot{
		NextFencingToken: nextToken,
		Leases:           make([]SQLShardLease, 0, int(count)),
	}
	for index := uint64(0); index < count; index++ {
		shardID, ok := reader.string(maxSQLShardLeaseIDBytes)
		if !ok {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
		owner, ok := reader.string(maxSQLShardLeaseOwnerBytes)
		if !ok {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
		token, ok := reader.uvarint()
		if !ok || token == 0 || token >= nextToken {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
		nanos, ok := reader.varint()
		if !ok {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
		snapshot.Leases = append(snapshot.Leases, SQLShardLease{
			ShardID:      shardID,
			Owner:        owner,
			FencingToken: token,
			ExpiresAt:    time.Unix(0, nanos).UTC(),
		})
	}
	if !reader.done() {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	return snapshot, nil
}

func (registry *SQLShardLeaseRegistry) validateIdentity(shardID, owner string) error {
	if registry == nil {
		return ErrSQLShardLeaseRegistryNil
	}
	if shardID == "" {
		return ErrSQLShardLeaseShardRequired
	}
	if len(shardID) > registry.maxShardIDBytes {
		return ErrSQLShardLeaseShardTooLarge
	}
	if owner == "" {
		return ErrSQLShardLeaseOwnerRequired
	}
	if len(owner) > registry.maxOwnerBytes {
		return ErrSQLShardLeaseOwnerTooLarge
	}
	return nil
}

func (registry *SQLShardLeaseRegistry) validateLease(lease SQLShardLease) error {
	if err := registry.validateIdentity(lease.ShardID, lease.Owner); err != nil {
		return err
	}
	if lease.FencingToken == 0 || lease.ExpiresAt.IsZero() {
		return ErrSQLShardLeaseSnapshotInvalid
	}
	return nil
}

func (registry *SQLShardLeaseRegistry) validateSnapshot(snapshot SQLShardLeaseSnapshot) (SQLShardLeaseSnapshot, error) {
	if snapshot.NextFencingToken == 0 || snapshot.NextFencingToken == 1 && len(snapshot.Leases) > 0 {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	if len(snapshot.Leases) > registry.maxShards || len(snapshot.Leases) > maxSQLShardLeaseSnapshotEntries {
		return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
	}
	validated := SQLShardLeaseSnapshot{
		NextFencingToken: snapshot.NextFencingToken,
		Leases:           append([]SQLShardLease(nil), snapshot.Leases...),
	}
	sort.Slice(validated.Leases, func(left, right int) bool { return validated.Leases[left].ShardID < validated.Leases[right].ShardID })
	for index, lease := range validated.Leases {
		if err := registry.validateLease(lease); err != nil {
			return SQLShardLeaseSnapshot{}, err
		}
		if lease.FencingToken >= validated.NextFencingToken {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
		if index > 0 && validated.Leases[index-1].ShardID == lease.ShardID {
			return SQLShardLeaseSnapshot{}, ErrSQLShardLeaseSnapshotInvalid
		}
	}
	return validated, nil
}

func (registry *SQLShardLeaseRegistry) allocateFencingToken() (uint64, error) {
	for {
		current := registry.nextToken.Load()
		if current == 0 || current == ^uint64(0) {
			return 0, ErrSQLShardLeaseFencingExhausted
		}
		if registry.nextToken.CompareAndSwap(current, current+1) {
			return current, nil
		}
	}
}

func (registry *SQLShardLeaseRegistry) shard(shardID string) *sqlShardLeaseShard {
	return &registry.shards[int(sqlShardLeaseHash(shardID)%uint64(len(registry.shards)))]
}

func (registry *SQLShardLeaseRegistry) pruneExpiredLocked(now time.Time) {
	for index := range registry.shards {
		shard := &registry.shards[index]
		shard.mu.Lock()
		for shardID, lease := range shard.leases {
			if lease.ExpiresAt.After(now) {
				continue
			}
			delete(shard.leases, shardID)
			registry.active.Add(-1)
		}
		shard.mu.Unlock()
	}
}

func (registry *SQLShardLeaseRegistry) lockShards() {
	for index := range registry.shards {
		registry.shards[index].mu.Lock()
	}
}

func (registry *SQLShardLeaseRegistry) unlockShards() {
	for index := len(registry.shards) - 1; index >= 0; index-- {
		registry.shards[index].mu.Unlock()
	}
}

func sqlShardLeaseHash(value string) uint64 {
	var hash uint64 = 14695981039346656037
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= 1099511628211
	}
	return hash
}

func marshalSQLShardLeaseSnapshot(snapshot SQLShardLeaseSnapshot) ([]byte, error) {
	if snapshot.NextFencingToken == 0 || len(snapshot.Leases) > maxSQLShardLeaseSnapshotEntries {
		return nil, ErrSQLShardLeaseSnapshotInvalid
	}
	data := make([]byte, 0, 16+len(snapshot.Leases)*64)
	data = append(data, 'H', 'S', 'L', '1', 1)
	data = appendSQLShardLeaseUvarint(data, snapshot.NextFencingToken)
	data = appendSQLShardLeaseUvarint(data, uint64(len(snapshot.Leases)))
	for index, lease := range snapshot.Leases {
		if index > 0 && snapshot.Leases[index-1].ShardID >= lease.ShardID {
			return nil, ErrSQLShardLeaseSnapshotInvalid
		}
		if lease.ShardID == "" || len(lease.ShardID) > maxSQLShardLeaseIDBytes || lease.Owner == "" || len(lease.Owner) > maxSQLShardLeaseOwnerBytes || lease.FencingToken == 0 || lease.FencingToken >= snapshot.NextFencingToken || lease.ExpiresAt.IsZero() {
			return nil, ErrSQLShardLeaseSnapshotInvalid
		}
		data = appendSQLShardLeaseString(data, lease.ShardID)
		data = appendSQLShardLeaseString(data, lease.Owner)
		data = appendSQLShardLeaseUvarint(data, lease.FencingToken)
		data = appendSQLShardLeaseVarint(data, lease.ExpiresAt.UnixNano())
		if len(data) > maxSQLShardLeaseMaxBytes {
			return nil, ErrSQLShardLeaseSnapshotInvalid
		}
	}
	if len(data)+4 > maxSQLShardLeaseMaxBytes {
		return nil, ErrSQLShardLeaseSnapshotInvalid
	}
	checksum := crc32.ChecksumIEEE(data)
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], checksum)
	data = append(data, encoded[:]...)
	return data, nil
}

func appendSQLShardLeaseString(data []byte, value string) []byte {
	data = appendSQLShardLeaseUvarint(data, uint64(len(value)))
	return append(data, value...)
}

func appendSQLShardLeaseUvarint(data []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(data, encoded[:length]...)
}

func appendSQLShardLeaseVarint(data []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutVarint(encoded[:], value)
	return append(data, encoded[:length]...)
}

type sqlShardLeaseReader struct {
	data   []byte
	offset int
}

func (reader *sqlShardLeaseReader) uvarint() (uint64, bool) {
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

func (reader *sqlShardLeaseReader) varint() (int64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, length := binary.Varint(reader.data[reader.offset:])
	if length <= 0 {
		return 0, false
	}
	reader.offset += length
	return value, true
}

func (reader *sqlShardLeaseReader) string(maxBytes int) (string, bool) {
	length, ok := reader.uvarint()
	if !ok || length == 0 || length > uint64(maxBytes) || length > uint64(len(reader.data)-reader.offset) {
		return "", false
	}
	start := reader.offset
	reader.offset += int(length)
	return string(reader.data[start:reader.offset]), true
}

func (reader *sqlShardLeaseReader) done() bool {
	return reader.offset == len(reader.data)
}
