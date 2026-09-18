package hatSql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSQLRowLockShardCount bounds unrelated key contention without
	// allocating a large per-key structure.
	DefaultSQLRowLockShardCount = 16
	// DefaultSQLRowLockMaxKeys prevents an accidental unbounded lock-key map.
	DefaultSQLRowLockMaxKeys = 65536
	// DefaultSQLRowLockMaxKeyBytes bounds caller-controlled key retention.
	DefaultSQLRowLockMaxKeyBytes = 256
	maxSQLRowLockShardCount      = 1024
)

var (
	ErrSQLRowLockManagerNil  = errors.New("SQL row lock manager is nil")
	ErrSQLRowLockContextNil  = errors.New("SQL row lock context is nil")
	ErrSQLRowLockKeyRequired = errors.New("SQL row lock key is required")
	ErrSQLRowLockKeyTooLarge = errors.New("SQL row lock key is too large")
	ErrSQLRowLockCapacity    = errors.New("SQL row lock key capacity exceeded")
)

// SQLRowLockManagerOptions bounds a row-lock manager. Zero values select the
// documented defaults.
type SQLRowLockManagerOptions struct {
	ShardCount  int
	MaxKeys     int
	MaxKeyBytes int
}

// SQLRowLockManagerStats is a point-in-time manager capacity snapshot.
type SQLRowLockManagerStats struct {
	Entries     int
	Shards      int
	MaxKeys     int
	MaxKeyBytes int
}

type sqlRowLockShard struct {
	mu      sync.Mutex
	entries map[string]*sqlRowLockEntry
}

type sqlRowLockEntry struct {
	token chan struct{}
	refs  int
}

// SQLRowLockManager provides bounded, per-key serialized leases. It is an
// importable primitive for callers implementing SELECT FOR UPDATE-style
// workflows; SQL grammar and transaction lifetime remain caller-owned.
type SQLRowLockManager struct {
	shards      []sqlRowLockShard
	maxKeys     int
	maxKeyBytes int

	capacityMu sync.Mutex
	entryCount atomic.Int64
}

// SQLRowLockLease releases one acquired key when Release is called. Release is
// idempotent and safe to call from cleanup paths.
type SQLRowLockLease struct {
	manager  *SQLRowLockManager
	shard    *sqlRowLockShard
	key      string
	entry    *sqlRowLockEntry
	released atomic.Bool
}

// NewSQLRowLockManager creates a bounded row-lock manager. It never changes
// ordinary SQL execution and has no process-global state.
func NewSQLRowLockManager(options SQLRowLockManagerOptions) *SQLRowLockManager {
	shardCount := options.ShardCount
	if shardCount <= 0 {
		shardCount = DefaultSQLRowLockShardCount
	}
	if shardCount > maxSQLRowLockShardCount {
		shardCount = maxSQLRowLockShardCount
	}
	maxKeys := options.MaxKeys
	if maxKeys <= 0 {
		maxKeys = DefaultSQLRowLockMaxKeys
	}
	maxKeyBytes := options.MaxKeyBytes
	if maxKeyBytes <= 0 {
		maxKeyBytes = DefaultSQLRowLockMaxKeyBytes
	}
	manager := &SQLRowLockManager{
		shards:      make([]sqlRowLockShard, shardCount),
		maxKeys:     maxKeys,
		maxKeyBytes: maxKeyBytes,
	}
	for index := range manager.shards {
		manager.shards[index].entries = make(map[string]*sqlRowLockEntry)
	}
	return manager
}

// Acquire waits until key is exclusively owned or ctx is canceled.
func (manager *SQLRowLockManager) Acquire(ctx context.Context, key string) (*SQLRowLockLease, error) {
	if ctx == nil {
		return nil, ErrSQLRowLockContextNil
	}
	return manager.acquire(ctx, key, false)
}

// TryAcquire returns a lease only when key is immediately available. A
// contended existing key returns (nil, nil); capacity and input failures are
// returned as errors.
func (manager *SQLRowLockManager) TryAcquire(key string) (*SQLRowLockLease, error) {
	return manager.acquire(nil, key, true)
}

// Stats returns bounded capacity metadata and the current live-key count.
func (manager *SQLRowLockManager) Stats() SQLRowLockManagerStats {
	if manager == nil {
		return SQLRowLockManagerStats{}
	}
	return SQLRowLockManagerStats{
		Entries:     int(manager.entryCount.Load()),
		Shards:      len(manager.shards),
		MaxKeys:     manager.maxKeys,
		MaxKeyBytes: manager.maxKeyBytes,
	}
}

func (manager *SQLRowLockManager) acquire(ctx context.Context, key string, tryOnly bool) (*SQLRowLockLease, error) {
	if manager == nil {
		return nil, ErrSQLRowLockManagerNil
	}
	if key == "" {
		return nil, ErrSQLRowLockKeyRequired
	}
	if len(key) > manager.maxKeyBytes {
		return nil, ErrSQLRowLockKeyTooLarge
	}
	shard := &manager.shards[sqlRowLockShardIndex(key, len(manager.shards))]
	shard.mu.Lock()
	entry := shard.entries[key]
	if entry == nil {
		if !manager.reserveEntry() {
			shard.mu.Unlock()
			return nil, ErrSQLRowLockCapacity
		}
		entry = &sqlRowLockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		shard.entries[key] = entry
	}
	entry.refs++
	shard.mu.Unlock()

	if tryOnly {
		select {
		case <-entry.token:
			return &SQLRowLockLease{manager: manager, shard: shard, key: key, entry: entry}, nil
		default:
			manager.releaseReference(shard, key, entry)
			return nil, nil
		}
	}
	select {
	case <-entry.token:
		if err := ctx.Err(); err != nil {
			entry.token <- struct{}{}
			manager.releaseReference(shard, key, entry)
			return nil, err
		}
		return &SQLRowLockLease{manager: manager, shard: shard, key: key, entry: entry}, nil
	case <-ctx.Done():
		manager.releaseReference(shard, key, entry)
		return nil, ctx.Err()
	}
}

func (manager *SQLRowLockManager) reserveEntry() bool {
	manager.capacityMu.Lock()
	defer manager.capacityMu.Unlock()
	if manager.entryCount.Load() >= int64(manager.maxKeys) {
		return false
	}
	manager.entryCount.Add(1)
	return true
}

func (manager *SQLRowLockManager) releaseReference(shard *sqlRowLockShard, key string, entry *sqlRowLockEntry) {
	shard.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && shard.entries[key] == entry {
		delete(shard.entries, key)
		manager.entryCount.Add(-1)
	}
	shard.mu.Unlock()
}

// Release returns the key to the next waiter. It is safe to call more than
// once; only the first call changes manager state.
func (lease *SQLRowLockLease) Release() bool {
	if lease == nil || lease.manager == nil || !lease.released.CompareAndSwap(false, true) {
		return false
	}
	lease.entry.token <- struct{}{}
	lease.manager.releaseReference(lease.shard, lease.key, lease.entry)
	return true
}

// Key returns the caller-supplied lock identity.
func (lease *SQLRowLockLease) Key() string {
	if lease == nil {
		return ""
	}
	return lease.key
}

func sqlRowLockShardIndex(key string, shardCount int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(key); index++ {
		hash ^= uint32(key[index])
		hash *= 16777619
	}
	return int(hash % uint32(shardCount))
}
