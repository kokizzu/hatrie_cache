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
	// DefaultSQLRowLockMaxOwnerBytes bounds caller-controlled transaction IDs.
	DefaultSQLRowLockMaxOwnerBytes = 128
	// DefaultSQLRowLockMaxWaitEdges bounds opt-in deadlock graph state.
	DefaultSQLRowLockMaxWaitEdges = 65536
	maxSQLRowLockShardCount       = 1024
	maxSQLRowLockOwnerBytes       = 4096
	maxSQLRowLockWaitEdges        = 1 << 20
)

var (
	ErrSQLRowLockManagerNil        = errors.New("SQL row lock manager is nil")
	ErrSQLRowLockContextNil        = errors.New("SQL row lock context is nil")
	ErrSQLRowLockKeyRequired       = errors.New("SQL row lock key is required")
	ErrSQLRowLockKeyTooLarge       = errors.New("SQL row lock key is too large")
	ErrSQLRowLockCapacity          = errors.New("SQL row lock key capacity exceeded")
	ErrSQLRowLockOwnerRequired     = errors.New("SQL row lock owner is required")
	ErrSQLRowLockOwnerTooLarge     = errors.New("SQL row lock owner is too large")
	ErrSQLRowLockDeadlock          = errors.New("SQL row lock deadlock detected")
	ErrSQLRowLockWaitGraphCapacity = errors.New("SQL row lock wait graph capacity exceeded")
)

// SQLRowLockManagerOptions bounds a row-lock manager. Zero values select the
// documented defaults.
type SQLRowLockManagerOptions struct {
	ShardCount              int
	MaxKeys                 int
	MaxKeyBytes             int
	EnableDeadlockDetection bool
	MaxOwnerBytes           int
	MaxWaitEdges            int
}

// SQLRowLockManagerStats is a point-in-time manager capacity snapshot.
type SQLRowLockManagerStats struct {
	Entries           int
	Shards            int
	MaxKeys           int
	MaxKeyBytes       int
	DeadlockDetection bool
	WaitEdges         int
	MaxWaitEdges      int
}

type sqlRowLockShard struct {
	mu      sync.Mutex
	entries map[string]*sqlRowLockEntry
}

type sqlRowLockEntry struct {
	token chan struct{}
	wake  chan struct{}
	refs  int
	held  bool
	owner string
}

type sqlRowLockWaitEdge struct {
	key     string
	blocker string
}

// SQLRowLockManager provides bounded, per-key serialized leases. It is an
// importable primitive for callers implementing SELECT FOR UPDATE-style
// workflows; SQL grammar and transaction lifetime remain caller-owned.
type SQLRowLockManager struct {
	shards            []sqlRowLockShard
	maxKeys           int
	maxKeyBytes       int
	deadlockDetection bool
	maxOwnerBytes     int
	maxWaitEdges      int
	deadlockMu        sync.Mutex
	waitEdges         map[string]map[string]sqlRowLockWaitEdge

	capacityMu sync.Mutex
	entryCount atomic.Int64
}

// SQLRowLockLease releases one acquired key when Release is called. Release is
// idempotent and safe to call from cleanup paths.
type SQLRowLockLease struct {
	manager  *SQLRowLockManager
	shard    *sqlRowLockShard
	key      string
	owner    string
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
	maxOwnerBytes := options.MaxOwnerBytes
	if maxOwnerBytes <= 0 {
		maxOwnerBytes = DefaultSQLRowLockMaxOwnerBytes
	}
	if maxOwnerBytes > maxSQLRowLockOwnerBytes {
		maxOwnerBytes = maxSQLRowLockOwnerBytes
	}
	maxWaitEdges := options.MaxWaitEdges
	if maxWaitEdges <= 0 {
		maxWaitEdges = DefaultSQLRowLockMaxWaitEdges
	}
	if maxWaitEdges > maxSQLRowLockWaitEdges {
		maxWaitEdges = maxSQLRowLockWaitEdges
	}
	manager := &SQLRowLockManager{
		shards:            make([]sqlRowLockShard, shardCount),
		maxKeys:           maxKeys,
		maxKeyBytes:       maxKeyBytes,
		deadlockDetection: options.EnableDeadlockDetection,
		maxOwnerBytes:     maxOwnerBytes,
		maxWaitEdges:      maxWaitEdges,
	}
	if manager.deadlockDetection {
		manager.waitEdges = make(map[string]map[string]sqlRowLockWaitEdge)
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
	return manager.acquire(ctx, key, false, "")
}

// AcquireOwned waits for key while associating the wait with owner. When
// deadlock detection is enabled, a cycle among owner-aware acquisitions is
// rejected before the caller blocks indefinitely. All acquisitions in a
// transaction must use the same stable owner identity for detection to help.
func (manager *SQLRowLockManager) AcquireOwned(ctx context.Context, owner, key string) (*SQLRowLockLease, error) {
	if ctx == nil {
		return nil, ErrSQLRowLockContextNil
	}
	if err := manager.validateOwner(owner); err != nil {
		return nil, err
	}
	return manager.acquire(ctx, key, false, owner)
}

// TryAcquire returns a lease only when key is immediately available. A
// contended existing key returns (nil, nil); capacity and input failures are
// returned as errors.
func (manager *SQLRowLockManager) TryAcquire(key string) (*SQLRowLockLease, error) {
	return manager.acquire(nil, key, true, "")
}

// TryAcquireOwned returns a lease only when key is immediately available and
// records owner for subsequent deadlock detection.
func (manager *SQLRowLockManager) TryAcquireOwned(owner, key string) (*SQLRowLockLease, error) {
	if err := manager.validateOwner(owner); err != nil {
		return nil, err
	}
	return manager.acquire(nil, key, true, owner)
}

// Stats returns bounded capacity metadata and the current live-key count.
func (manager *SQLRowLockManager) Stats() SQLRowLockManagerStats {
	if manager == nil {
		return SQLRowLockManagerStats{}
	}
	return SQLRowLockManagerStats{
		Entries:           int(manager.entryCount.Load()),
		Shards:            len(manager.shards),
		MaxKeys:           manager.maxKeys,
		MaxKeyBytes:       manager.maxKeyBytes,
		DeadlockDetection: manager.deadlockDetection,
		WaitEdges:         manager.waitEdgeCount(),
		MaxWaitEdges:      manager.maxWaitEdges,
	}
}

func (manager *SQLRowLockManager) acquire(ctx context.Context, key string, tryOnly bool, owner string) (*SQLRowLockLease, error) {
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
		if manager.deadlockDetection {
			entry.wake = make(chan struct{})
		}
		entry.token <- struct{}{}
		shard.entries[key] = entry
	}
	entry.refs++
	shard.mu.Unlock()

	if tryOnly {
		select {
		case <-entry.token:
			manager.markEntryAcquired(entry, key, owner)
			return &SQLRowLockLease{manager: manager, shard: shard, key: key, owner: owner, entry: entry}, nil
		default:
			manager.releaseReference(shard, key, entry)
			return nil, nil
		}
	}
	if manager.deadlockDetection && owner != "" {
		return manager.acquireWithDeadlock(ctx, owner, key, shard, entry)
	}
	select {
	case <-entry.token:
		if err := ctx.Err(); err != nil {
			entry.token <- struct{}{}
			manager.releaseReference(shard, key, entry)
			return nil, err
		}
		manager.markEntryAcquired(entry, key, owner)
		return &SQLRowLockLease{manager: manager, shard: shard, key: key, owner: owner, entry: entry}, nil
	case <-ctx.Done():
		manager.releaseReference(shard, key, entry)
		return nil, ctx.Err()
	}
}

func (manager *SQLRowLockManager) acquireWithDeadlock(ctx context.Context, owner, key string, shard *sqlRowLockShard, entry *sqlRowLockEntry) (*SQLRowLockLease, error) {
	for {
		manager.deadlockMu.Lock()
		blocker := entry.owner
		wake := entry.wake
		if entry.held && blocker != "" {
			if manager.wouldDeadlockLocked(owner, blocker) {
				manager.deadlockMu.Unlock()
				manager.releaseReference(shard, key, entry)
				return nil, ErrSQLRowLockDeadlock
			}
			if !manager.addWaitEdgeLocked(owner, key, blocker) {
				manager.deadlockMu.Unlock()
				manager.releaseReference(shard, key, entry)
				return nil, ErrSQLRowLockWaitGraphCapacity
			}
		}
		manager.deadlockMu.Unlock()

		select {
		case <-entry.token:
			if err := ctx.Err(); err != nil {
				manager.removeWaitEdge(owner, key)
				entry.token <- struct{}{}
				manager.releaseReference(shard, key, entry)
				return nil, err
			}
			manager.markEntryAcquired(entry, key, owner)
			return &SQLRowLockLease{manager: manager, shard: shard, key: key, owner: owner, entry: entry}, nil
		case <-ctx.Done():
			manager.removeWaitEdge(owner, key)
			manager.releaseReference(shard, key, entry)
			return nil, ctx.Err()
		case <-wake:
			manager.removeWaitEdge(owner, key)
		}
	}
}

func (manager *SQLRowLockManager) validateOwner(owner string) error {
	if manager == nil {
		return ErrSQLRowLockManagerNil
	}
	if owner == "" {
		return ErrSQLRowLockOwnerRequired
	}
	if len(owner) > manager.maxOwnerBytes {
		return ErrSQLRowLockOwnerTooLarge
	}
	return nil
}

func (manager *SQLRowLockManager) markEntryAcquired(entry *sqlRowLockEntry, key, owner string) {
	if !manager.deadlockDetection {
		return
	}
	manager.deadlockMu.Lock()
	entry.held = true
	entry.owner = owner
	manager.removeWaitEdgeLocked(owner, key)
	manager.deadlockMu.Unlock()
}

func (manager *SQLRowLockManager) releaseEntryState(entry *sqlRowLockEntry, key string) {
	if !manager.deadlockDetection {
		return
	}
	manager.deadlockMu.Lock()
	entry.held = false
	entry.owner = ""
	manager.removeWaitEdgesForKeyLocked(key)
	oldWake := entry.wake
	entry.wake = make(chan struct{})
	close(oldWake)
	manager.deadlockMu.Unlock()
}

func (manager *SQLRowLockManager) addWaitEdgeLocked(owner, key, blocker string) bool {
	byKey := manager.waitEdges[owner]
	if byKey == nil {
		if manager.waitEdgeCountLocked() >= manager.maxWaitEdges {
			return false
		}
		byKey = make(map[string]sqlRowLockWaitEdge)
		manager.waitEdges[owner] = byKey
	} else if _, exists := byKey[key]; !exists && manager.waitEdgeCountLocked() >= manager.maxWaitEdges {
		return false
	}
	byKey[key] = sqlRowLockWaitEdge{key: key, blocker: blocker}
	return true
}

func (manager *SQLRowLockManager) removeWaitEdge(owner, key string) {
	if !manager.deadlockDetection || owner == "" {
		return
	}
	manager.deadlockMu.Lock()
	manager.removeWaitEdgeLocked(owner, key)
	manager.deadlockMu.Unlock()
}

func (manager *SQLRowLockManager) removeWaitEdgeLocked(owner, key string) {
	if owner == "" {
		return
	}
	byKey := manager.waitEdges[owner]
	if byKey == nil {
		return
	}
	delete(byKey, key)
	if len(byKey) == 0 {
		delete(manager.waitEdges, owner)
	}
}

func (manager *SQLRowLockManager) removeWaitEdgesForKeyLocked(key string) {
	for owner, byKey := range manager.waitEdges {
		delete(byKey, key)
		if len(byKey) == 0 {
			delete(manager.waitEdges, owner)
		}
	}
}

func (manager *SQLRowLockManager) wouldDeadlockLocked(owner, blocker string) bool {
	if owner == blocker {
		return true
	}
	seen := map[string]struct{}{owner: {}, blocker: {}}
	stack := []string{blocker}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, edge := range manager.waitEdges[current] {
			if edge.blocker == owner {
				return true
			}
			if _, ok := seen[edge.blocker]; ok {
				continue
			}
			seen[edge.blocker] = struct{}{}
			stack = append(stack, edge.blocker)
		}
	}
	return false
}

func (manager *SQLRowLockManager) waitEdgeCount() int {
	if manager == nil || !manager.deadlockDetection {
		return 0
	}
	manager.deadlockMu.Lock()
	defer manager.deadlockMu.Unlock()
	return manager.waitEdgeCountLocked()
}

func (manager *SQLRowLockManager) waitEdgeCountLocked() int {
	count := 0
	for _, byKey := range manager.waitEdges {
		count += len(byKey)
	}
	return count
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
	lease.manager.releaseEntryState(lease.entry, lease.key)
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
