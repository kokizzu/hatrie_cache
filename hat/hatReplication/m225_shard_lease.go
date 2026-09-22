package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultShardLeaseMaxLeases bounds the number of shards tracked by a
	// registry when no explicit bound is supplied.
	DefaultShardLeaseMaxLeases = 1024

	maxShardLeaseMaxLeases       = 1 << 20
	MaxShardLeaseIdentityBytes   = 256
	MaxShardLeaseTTL             = 365 * 24 * time.Hour
	maxShardLeaseSnapshotEntries = 1 << 20
	maxShardLeaseSnapshotBytes   = 64 << 20
	shardLeaseSnapshotVersion    = 1
)

var (
	// ErrShardLeaseRegistryNil indicates a method call on a nil registry.
	ErrShardLeaseRegistryNil = errors.New("hatriecache: shard lease registry is nil")
	// ErrShardLeaseOptionsInvalid identifies an invalid lease registry bound.
	ErrShardLeaseOptionsInvalid = errors.New("hatriecache: shard lease options are invalid")
	// ErrShardLeaseInvalid identifies an invalid shard, owner, lease, or TTL.
	ErrShardLeaseInvalid = errors.New("hatriecache: shard lease is invalid")
	// ErrShardLeaseHeld indicates that the shard is currently owned by another
	// lease. Acquire is intentionally not re-entrant, even for the same owner.
	ErrShardLeaseHeld = errors.New("hatriecache: shard lease is held")
	// ErrShardLeaseCapacity indicates that the configured shard bound is full.
	ErrShardLeaseCapacity = errors.New("hatriecache: shard lease capacity is full")
	// ErrShardLeaseFenced indicates that a lease token no longer owns its shard.
	ErrShardLeaseFenced = errors.New("hatriecache: shard lease is fenced")
	// ErrShardLeaseExpired indicates that a matching lease reached its expiry.
	ErrShardLeaseExpired = errors.New("hatriecache: shard lease is expired")
	// ErrShardLeaseTokenExhausted indicates that no next fencing token exists.
	ErrShardLeaseTokenExhausted = errors.New("hatriecache: shard lease fencing token is exhausted")
	// ErrShardLeaseSnapshotInvalid indicates a corrupt or unsupported snapshot.
	ErrShardLeaseSnapshotInvalid = errors.New("hatriecache: shard lease snapshot is invalid")
)

// ShardLeaseRegistryOptions bounds the number of shard leases held by one
// registry. The registry is process-local; persistence is supplied through
// Snapshot and the binary snapshot methods below.
type ShardLeaseRegistryOptions struct {
	// MaxLeases is the maximum number of shard names. Zero uses
	// DefaultShardLeaseMaxLeases.
	MaxLeases int
}

// ShardLease identifies the owner and fencing token currently associated with
// one shard. A caller must include both Owner and FencingToken when renewing
// or releasing a lease and should pass FencingToken to every state mutation
// performed for the shard.
type ShardLease struct {
	Shard        string
	Owner        string
	FencingToken uint64
	ExpiresAt    time.Time
}

// ShardLeaseRegistrySnapshot is a detached, sorted view of a registry. The
// last fencing token is retained even after a lease is released so restore
// cannot reuse a token that an old owner may still present.
type ShardLeaseRegistrySnapshot struct {
	LastFencingToken uint64
	Leases           []ShardLease
}

// ShardLeaseRegistry serializes lease acquisition and renewal within one
// process. It does not itself provide cross-process atomic persistence: the
// embedding service must save MarshalBinary output atomically and restore it
// before admitting work after restart.
type ShardLeaseRegistry struct {
	mu               sync.RWMutex
	maxLeases        int
	lastFencingToken uint64
	leases           map[string]ShardLease
}

// NewShardLeaseRegistry creates an empty shard lease registry.
func NewShardLeaseRegistry(options ShardLeaseRegistryOptions) (*ShardLeaseRegistry, error) {
	maxLeases, err := validateShardLeaseOptions(options)
	if err != nil {
		return nil, err
	}
	return &ShardLeaseRegistry{
		maxLeases: maxLeases,
		leases:    make(map[string]ShardLease),
	}, nil
}

// NewShardLeaseRegistryFromSnapshot creates a registry and restores its
// persisted fencing history before returning it.
func NewShardLeaseRegistryFromSnapshot(options ShardLeaseRegistryOptions, snapshot ShardLeaseRegistrySnapshot) (*ShardLeaseRegistry, error) {
	registry, err := NewShardLeaseRegistry(options)
	if err != nil {
		return nil, err
	}
	if err := registry.Restore(snapshot); err != nil {
		return nil, err
	}
	return registry, nil
}

// Acquire obtains a new fencing token for shard when no unexpired lease owns
// it. Expired leases are removed before the capacity check. A successful
// takeover always receives a strictly larger token than every prior lease.
func (registry *ShardLeaseRegistry) Acquire(shard, owner string, ttl time.Duration, now time.Time) (ShardLease, error) {
	if registry == nil {
		return ShardLease{}, ErrShardLeaseRegistryNil
	}
	shard, owner, err := normalizeShardLeaseIdentity(shard, owner)
	if err != nil {
		return ShardLease{}, err
	}
	if err := validateShardLeaseTTL(ttl); err != nil {
		return ShardLease{}, err
	}
	now = normalizeShardLeaseTime(now)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.reapExpiredLocked(now)
	if _, exists := registry.leases[shard]; exists {
		return ShardLease{}, fmt.Errorf("%w: %q", ErrShardLeaseHeld, shard)
	}
	if len(registry.leases) >= registry.maxLeases {
		return ShardLease{}, ErrShardLeaseCapacity
	}
	if registry.lastFencingToken == ^uint64(0) {
		return ShardLease{}, ErrShardLeaseTokenExhausted
	}
	registry.lastFencingToken++
	lease := ShardLease{
		Shard:        shard,
		Owner:        owner,
		FencingToken: registry.lastFencingToken,
		ExpiresAt:    now.Add(ttl),
	}
	registry.leases[shard] = lease
	return lease, nil
}

// Renew extends a current lease without changing its fencing token. A lease
// that has expired cannot be renewed, even if no other owner has taken it.
func (registry *ShardLeaseRegistry) Renew(lease ShardLease, ttl time.Duration, now time.Time) (ShardLease, error) {
	if registry == nil {
		return ShardLease{}, ErrShardLeaseRegistryNil
	}
	if err := validateShardLeaseReference(lease); err != nil {
		return ShardLease{}, err
	}
	if err := validateShardLeaseTTL(ttl); err != nil {
		return ShardLease{}, err
	}
	now = normalizeShardLeaseTime(now)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, exists := registry.leases[lease.Shard]
	if !exists || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken {
		return ShardLease{}, ErrShardLeaseFenced
	}
	if !current.ExpiresAt.After(now) {
		delete(registry.leases, lease.Shard)
		return ShardLease{}, ErrShardLeaseExpired
	}
	current.ExpiresAt = now.Add(ttl)
	registry.leases[lease.Shard] = current
	return current, nil
}

// Release removes a current lease. A stale owner cannot release a replacement
// lease because the fencing token must match exactly.
func (registry *ShardLeaseRegistry) Release(lease ShardLease, now time.Time) error {
	if registry == nil {
		return ErrShardLeaseRegistryNil
	}
	if err := validateShardLeaseReference(lease); err != nil {
		return err
	}
	now = normalizeShardLeaseTime(now)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, exists := registry.leases[lease.Shard]
	if !exists || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken {
		return ErrShardLeaseFenced
	}
	if !current.ExpiresAt.After(now) {
		delete(registry.leases, lease.Shard)
		return ErrShardLeaseExpired
	}
	delete(registry.leases, lease.Shard)
	return nil
}

// Get returns the current unexpired lease for shard. Expired leases are
// treated as absent and are reaped by a later mutation.
func (registry *ShardLeaseRegistry) Get(shard string, now time.Time) (ShardLease, bool) {
	if registry == nil {
		return ShardLease{}, false
	}
	shard = strings.TrimSpace(shard)
	if !validShardLeaseIdentityPart(shard) {
		return ShardLease{}, false
	}
	now = normalizeShardLeaseTime(now)
	registry.mu.RLock()
	lease, exists := registry.leases[shard]
	registry.mu.RUnlock()
	if !exists || !lease.ExpiresAt.After(now) {
		return ShardLease{}, false
	}
	return lease, true
}

// Validate confirms that lease is still the current unexpired lease for its
// shard. Callers can use this fencing check before applying a state mutation
// whose storage layer cannot carry the token itself.
func (registry *ShardLeaseRegistry) Validate(lease ShardLease, now time.Time) error {
	if registry == nil {
		return ErrShardLeaseRegistryNil
	}
	if err := validateShardLeaseReference(lease); err != nil {
		return err
	}
	now = normalizeShardLeaseTime(now)
	registry.mu.RLock()
	current, exists := registry.leases[lease.Shard]
	registry.mu.RUnlock()
	if !exists || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken {
		return ErrShardLeaseFenced
	}
	if !current.ExpiresAt.After(now) {
		return ErrShardLeaseExpired
	}
	return nil
}

// Snapshot returns a detached shard-sorted view, including expired entries so
// a persistence cycle retains the exact lease history until the next acquire.
func (registry *ShardLeaseRegistry) Snapshot() ShardLeaseRegistrySnapshot {
	if registry == nil {
		return ShardLeaseRegistrySnapshot{}
	}
	registry.mu.RLock()
	snapshot := ShardLeaseRegistrySnapshot{
		LastFencingToken: registry.lastFencingToken,
		Leases:           make([]ShardLease, 0, len(registry.leases)),
	}
	for _, lease := range registry.leases {
		snapshot.Leases = append(snapshot.Leases, lease)
	}
	registry.mu.RUnlock()
	sortShardLeases(snapshot.Leases)
	return snapshot
}

// Restore atomically replaces the registry state after validating all lease
// identities, fencing tokens, and bounds.
func (registry *ShardLeaseRegistry) Restore(snapshot ShardLeaseRegistrySnapshot) error {
	if registry == nil {
		return ErrShardLeaseRegistryNil
	}
	leases, err := validateAndCloneShardLeaseSnapshot(snapshot, registry.maxLeases)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	registry.leases = leases
	registry.lastFencingToken = snapshot.LastFencingToken
	registry.mu.Unlock()
	return nil
}

// MarshalBinary encodes the current registry as a deterministic compact
// snapshot. It implements encoding.BinaryMarshaler for storage adapters.
func (registry *ShardLeaseRegistry) MarshalBinary() ([]byte, error) {
	if registry == nil {
		return nil, ErrShardLeaseRegistryNil
	}
	return registry.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and atomically restores a registry snapshot.
func (registry *ShardLeaseRegistry) UnmarshalBinary(data []byte) error {
	if registry == nil {
		return ErrShardLeaseRegistryNil
	}
	var snapshot ShardLeaseRegistrySnapshot
	if err := snapshot.UnmarshalBinary(data); err != nil {
		return err
	}
	return registry.Restore(snapshot)
}

// MarshalBinary encodes a snapshot as HSL1. Entries are sorted by shard and
// string lengths are bounded before encoding, keeping the format deterministic
// and avoiding JSON field overhead for persistence-heavy control state.
func (snapshot ShardLeaseRegistrySnapshot) MarshalBinary() ([]byte, error) {
	if err := validateShardLeaseSnapshot(snapshot, maxShardLeaseSnapshotEntries); err != nil {
		return nil, err
	}
	leases := append([]ShardLease(nil), snapshot.Leases...)
	sortShardLeases(leases)
	data := make([]byte, 0, shardLeaseSnapshotHeaderBytes+len(leases)*32)
	data = append(data, 'H', 'S', 'L', '1', shardLeaseSnapshotVersion)
	data = appendUint64(data, snapshot.LastFencingToken)
	data = appendUint32(data, uint32(len(leases)))
	for _, lease := range leases {
		data = appendShardLeaseString(data, lease.Shard)
		data = appendShardLeaseString(data, lease.Owner)
		data = appendUint64(data, lease.FencingToken)
		data = appendUint64(data, uint64(lease.ExpiresAt.UnixNano()))
	}
	if len(data) > maxShardLeaseSnapshotBytes {
		return nil, ErrShardLeaseSnapshotInvalid
	}
	return data, nil
}

// UnmarshalBinary decodes a strict HSL1 snapshot and rejects trailing bytes,
// duplicate shards, duplicate fencing tokens, and values over format bounds.
func (snapshot *ShardLeaseRegistrySnapshot) UnmarshalBinary(data []byte) error {
	if snapshot == nil || len(data) < shardLeaseSnapshotHeaderBytes || len(data) > maxShardLeaseSnapshotBytes {
		return ErrShardLeaseSnapshotInvalid
	}
	if string(data[:4]) != "HSL1" || data[4] != shardLeaseSnapshotVersion {
		return ErrShardLeaseSnapshotInvalid
	}
	offset := 5
	lastFencingToken, ok := readShardLeaseUint64(data, &offset)
	if !ok {
		return ErrShardLeaseSnapshotInvalid
	}
	count, ok := readShardLeaseUint32(data, &offset)
	if !ok || count > maxShardLeaseSnapshotEntries {
		return ErrShardLeaseSnapshotInvalid
	}
	leases := make([]ShardLease, 0, count)
	for index := uint32(0); index < count; index++ {
		shard, ok := readShardLeaseString(data, &offset)
		if !ok {
			return ErrShardLeaseSnapshotInvalid
		}
		owner, ok := readShardLeaseString(data, &offset)
		if !ok {
			return ErrShardLeaseSnapshotInvalid
		}
		fencingToken, ok := readShardLeaseUint64(data, &offset)
		if !ok {
			return ErrShardLeaseSnapshotInvalid
		}
		expiresAtUnixNano, ok := readShardLeaseUint64(data, &offset)
		if !ok {
			return ErrShardLeaseSnapshotInvalid
		}
		leases = append(leases, ShardLease{
			Shard:        shard,
			Owner:        owner,
			FencingToken: fencingToken,
			ExpiresAt:    time.Unix(0, int64(expiresAtUnixNano)).UTC(),
		})
	}
	if offset != len(data) {
		return ErrShardLeaseSnapshotInvalid
	}
	if err := validateShardLeaseSnapshot(ShardLeaseRegistrySnapshot{
		LastFencingToken: lastFencingToken,
		Leases:           leases,
	}, maxShardLeaseSnapshotEntries); err != nil {
		return ErrShardLeaseSnapshotInvalid
	}
	snapshot.LastFencingToken = lastFencingToken
	sortShardLeases(leases)
	snapshot.Leases = leases
	return nil
}

const shardLeaseSnapshotHeaderBytes = 5 + 8 + 4

func validateShardLeaseOptions(options ShardLeaseRegistryOptions) (int, error) {
	maxLeases := options.MaxLeases
	if maxLeases == 0 {
		return DefaultShardLeaseMaxLeases, nil
	}
	if maxLeases < 1 || maxLeases > maxShardLeaseMaxLeases {
		return 0, ErrShardLeaseOptionsInvalid
	}
	return maxLeases, nil
}

func normalizeShardLeaseIdentity(shard, owner string) (string, string, error) {
	shard = strings.TrimSpace(shard)
	owner = strings.TrimSpace(owner)
	if !validShardLeaseIdentityPart(shard) || !validShardLeaseIdentityPart(owner) {
		return "", "", ErrShardLeaseInvalid
	}
	return shard, owner, nil
}

func validShardLeaseIdentityPart(value string) bool {
	return value != "" && len(value) <= MaxShardLeaseIdentityBytes && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func validateShardLeaseTTL(ttl time.Duration) error {
	if ttl <= 0 || ttl > MaxShardLeaseTTL {
		return ErrShardLeaseInvalid
	}
	return nil
}

func validateShardLeaseReference(lease ShardLease) error {
	if !validShardLeaseIdentityPart(lease.Shard) || !validShardLeaseIdentityPart(lease.Owner) || lease.FencingToken == 0 || lease.ExpiresAt.IsZero() {
		return ErrShardLeaseInvalid
	}
	return nil
}

func normalizeShardLeaseTime(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

func (registry *ShardLeaseRegistry) reapExpiredLocked(now time.Time) {
	for shard, lease := range registry.leases {
		if !lease.ExpiresAt.After(now) {
			delete(registry.leases, shard)
		}
	}
}

func validateAndCloneShardLeaseSnapshot(snapshot ShardLeaseRegistrySnapshot, maxLeases int) (map[string]ShardLease, error) {
	if err := validateShardLeaseSnapshot(snapshot, maxLeases); err != nil {
		return nil, err
	}
	leases := make(map[string]ShardLease, len(snapshot.Leases))
	for _, lease := range snapshot.Leases {
		leases[lease.Shard] = ShardLease{
			Shard:        lease.Shard,
			Owner:        lease.Owner,
			FencingToken: lease.FencingToken,
			ExpiresAt:    lease.ExpiresAt.UTC(),
		}
	}
	return leases, nil
}

func validateShardLeaseSnapshot(snapshot ShardLeaseRegistrySnapshot, maxLeases int) error {
	if maxLeases < 1 || len(snapshot.Leases) > maxLeases || len(snapshot.Leases) > maxShardLeaseSnapshotEntries {
		return ErrShardLeaseSnapshotInvalid
	}
	shards := make(map[string]struct{}, len(snapshot.Leases))
	tokens := make(map[uint64]struct{}, len(snapshot.Leases))
	for _, lease := range snapshot.Leases {
		if err := validateShardLeaseReference(lease); err != nil || lease.FencingToken > snapshot.LastFencingToken {
			return ErrShardLeaseSnapshotInvalid
		}
		if _, exists := shards[lease.Shard]; exists {
			return ErrShardLeaseSnapshotInvalid
		}
		if _, exists := tokens[lease.FencingToken]; exists {
			return ErrShardLeaseSnapshotInvalid
		}
		shards[lease.Shard] = struct{}{}
		tokens[lease.FencingToken] = struct{}{}
	}
	if len(snapshot.Leases) == 0 && snapshot.LastFencingToken == 0 {
		return nil
	}
	if len(snapshot.Leases) > 0 && snapshot.LastFencingToken == 0 {
		return ErrShardLeaseSnapshotInvalid
	}
	return nil
}

func sortShardLeases(leases []ShardLease) {
	sort.Slice(leases, func(left, right int) bool { return leases[left].Shard < leases[right].Shard })
}

func appendUint32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendUint64(data []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendShardLeaseString(data []byte, value string) []byte {
	data = appendUint32(data, uint32(len(value)))
	return append(data, value...)
}

func readShardLeaseUint32(data []byte, offset *int) (uint32, bool) {
	if *offset < 0 || len(data)-*offset < 4 {
		return 0, false
	}
	value := binary.BigEndian.Uint32(data[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readShardLeaseUint64(data []byte, offset *int) (uint64, bool) {
	if *offset < 0 || len(data)-*offset < 8 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return value, true
}

func readShardLeaseString(data []byte, offset *int) (string, bool) {
	length, ok := readShardLeaseUint32(data, offset)
	if !ok || length == 0 || length > MaxShardLeaseIdentityBytes || uint64(length) > uint64(len(data)-*offset) {
		return "", false
	}
	end := *offset + int(length)
	value := string(data[*offset:end])
	*offset = end
	if !validShardLeaseIdentityPart(value) {
		return "", false
	}
	return value, true
}
