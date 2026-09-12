package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sqlQuotaShardCount     = 32
	sqlQuotaBucketCount    = 64
	sqlQuotaDefaultWindow  = time.Minute
	sqlQuotaDefaultMaxKeys = 4096
)

var (
	// ErrSQLQuotaExceeded indicates that a key exhausted a query, result-byte,
	// or execution-time quota in its sliding window.
	ErrSQLQuotaExceeded = errors.New("SQL quota exceeded")
	// ErrSQLQuotaKeysExceeded indicates that the registry reached its bounded
	// number of distinct active quota keys.
	ErrSQLQuotaKeysExceeded = errors.New("SQL quota key limit exceeded")
)

// SQLQuotaLimits defines rolling limits for one key. A zero component is
// disabled. Window defaults to one minute when zero.
type SQLQuotaLimits struct {
	MaxQueries       int
	MaxResultBytes   int64
	MaxExecutionTime time.Duration
	Window           time.Duration
}

// SQLQuotaRegistryOptions configures an opt-in, bounded quota registry.
type SQLQuotaRegistryOptions struct {
	Limits  SQLQuotaLimits
	MaxKeys int
	Now     func() time.Time
}

// SQLQuotaRegistry enforces independent sliding-window quotas for caller keys.
// It has no worker and allocates state only for keys that are used.
type SQLQuotaRegistry struct {
	limits      SQLQuotaLimits
	maxKeys     int64
	bucketWidth time.Duration
	now         func() time.Time
	keys        atomic.Int64
	shards      [sqlQuotaShardCount]sqlQuotaShard
}

type sqlQuotaShard struct {
	mu      sync.Mutex
	entries map[string]*sqlQuotaKeyState
}

type sqlQuotaKeyState struct {
	buckets []sqlQuotaBucket
	queries int
	bytes   int64
	elapsed int64
	active  int
}

type sqlQuotaBucket struct {
	start   int64
	queries int
	bytes   int64
	elapsed int64
}

// SQLQuotaReservation accounts one admitted query. Call Finish exactly once
// after the query completes; a repeated call is harmless.
type SQLQuotaReservation struct {
	registry *SQLQuotaRegistry
	key      string
	shard    int
	done     atomic.Bool
}

// NewSQLQuotaRegistry creates a bounded quota registry. Limits are disabled
// independently when their values are zero, so callers may begin with only a
// query-count policy and add byte/time limits later.
func NewSQLQuotaRegistry(options SQLQuotaRegistryOptions) (*SQLQuotaRegistry, error) {
	limits := options.Limits
	if limits.MaxQueries < 0 || limits.MaxResultBytes < 0 || limits.MaxExecutionTime < 0 || limits.Window < 0 {
		return nil, fmt.Errorf("SQL quota limits cannot be negative")
	}
	if limits.Window == 0 {
		limits.Window = sqlQuotaDefaultWindow
	}
	maxKeys := options.MaxKeys
	if maxKeys == 0 {
		maxKeys = sqlQuotaDefaultMaxKeys
	}
	if maxKeys < 1 {
		return nil, fmt.Errorf("SQL quota MaxKeys must be positive")
	}
	bucketWidth := limits.Window / sqlQuotaBucketCount
	if bucketWidth < time.Nanosecond {
		bucketWidth = time.Nanosecond
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &SQLQuotaRegistry{
		limits:      limits,
		maxKeys:     int64(maxKeys),
		bucketWidth: bucketWidth,
		now:         now,
	}, nil
}

// Begin admits one query for key. Empty keys use the bounded "default" key.
func (registry *SQLQuotaRegistry) Begin(key string) (*SQLQuotaReservation, error) {
	reservation, err := registry.begin(key)
	if err != nil {
		return nil, err
	}
	return &reservation, nil
}

func (registry *SQLQuotaRegistry) begin(key string) (SQLQuotaReservation, error) {
	if registry == nil {
		return SQLQuotaReservation{}, fmt.Errorf("SQL quota registry is required")
	}
	key = normalizeSQLQuotaKey(key)
	shardIndex := sqlQuotaShardIndex(key)
	shard := &registry.shards[shardIndex]
	now := registry.now()
	shard.mu.Lock()
	state := shard.entries[key]
	if state != nil {
		pruneSQLQuotaState(state, now, registry.limits.Window)
		if state.active == 0 && state.queries == 0 {
			delete(shard.entries, key)
			registry.keys.Add(-1)
			state = nil
		}
	}
	if state == nil {
		if !registry.reserveSQLQuotaKey() {
			shard.mu.Unlock()
			registry.pruneExpiredSQLQuotaKeys(now)
			shard.mu.Lock()
			state = shard.entries[key]
			if state != nil {
				pruneSQLQuotaState(state, now, registry.limits.Window)
			}
			if state == nil && !registry.reserveSQLQuotaKey() {
				shard.mu.Unlock()
				return SQLQuotaReservation{}, ErrSQLQuotaKeysExceeded
			}
		}
		if state == nil {
			if shard.entries == nil {
				shard.entries = make(map[string]*sqlQuotaKeyState)
			}
			state = &sqlQuotaKeyState{buckets: make([]sqlQuotaBucket, sqlQuotaBucketCount)}
			shard.entries[key] = state
		}
	}
	if registry.limits.MaxQueries > 0 && state.queries+state.active >= registry.limits.MaxQueries ||
		registry.limits.MaxResultBytes > 0 && state.bytes >= registry.limits.MaxResultBytes ||
		registry.limits.MaxExecutionTime > 0 && state.elapsed >= int64(registry.limits.MaxExecutionTime) {
		shard.mu.Unlock()
		return SQLQuotaReservation{}, ErrSQLQuotaExceeded
	}
	state.active++
	shard.mu.Unlock()
	return SQLQuotaReservation{registry: registry, key: key, shard: shardIndex}, nil
}

// Finish records resultBytes and elapsed execution time and reports whether
// the completed query pushed the key over any configured cumulative limit.
func (reservation *SQLQuotaReservation) Finish(resultBytes int64, elapsed time.Duration) error {
	if reservation == nil || reservation.registry == nil {
		return nil
	}
	if resultBytes < 0 || elapsed < 0 {
		return fmt.Errorf("SQL quota usage cannot be negative")
	}
	if reservation.done.Swap(true) {
		return nil
	}
	return reservation.finish(resultBytes, elapsed)
}

func (reservation *SQLQuotaReservation) finish(resultBytes int64, elapsed time.Duration) error {
	registry := reservation.registry
	shard := &registry.shards[reservation.shard]
	now := registry.now()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	state := shard.entries[reservation.key]
	if state == nil {
		return nil
	}
	pruneSQLQuotaState(state, now, registry.limits.Window)
	bucket := currentSQLQuotaBucket(state, now, registry.bucketWidth)
	if state.active > 0 {
		state.active--
	}
	bucket.queries++
	bucket.bytes += resultBytes
	bucket.elapsed += int64(elapsed)
	state.queries++
	state.bytes += resultBytes
	state.elapsed += int64(elapsed)
	if registry.limits.MaxResultBytes > 0 && state.bytes > registry.limits.MaxResultBytes ||
		registry.limits.MaxExecutionTime > 0 && state.elapsed > int64(registry.limits.MaxExecutionTime) {
		return ErrSQLQuotaExceeded
	}
	return nil
}

func (registry *SQLQuotaRegistry) reserveSQLQuotaKey() bool {
	for {
		current := registry.keys.Load()
		if current >= registry.maxKeys {
			return false
		}
		if registry.keys.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (registry *SQLQuotaRegistry) pruneExpiredSQLQuotaKeys(now time.Time) {
	for index := range registry.shards {
		shard := &registry.shards[index]
		shard.mu.Lock()
		for key, state := range shard.entries {
			pruneSQLQuotaState(state, now, registry.limits.Window)
			if state.active == 0 && state.queries == 0 {
				delete(shard.entries, key)
				registry.keys.Add(-1)
			}
		}
		shard.mu.Unlock()
	}
}

func pruneSQLQuotaState(state *sqlQuotaKeyState, now time.Time, window time.Duration) {
	cutoff := now.Add(-window).UnixNano()
	for index := range state.buckets {
		bucket := &state.buckets[index]
		if bucket.queries == 0 && bucket.bytes == 0 && bucket.elapsed == 0 || bucket.start > cutoff {
			continue
		}
		state.queries -= bucket.queries
		state.bytes -= bucket.bytes
		state.elapsed -= bucket.elapsed
		*bucket = sqlQuotaBucket{}
	}
}

func currentSQLQuotaBucket(state *sqlQuotaKeyState, now time.Time, width time.Duration) *sqlQuotaBucket {
	start := now.Truncate(width).UnixNano()
	index := int((start / int64(width)) % sqlQuotaBucketCount)
	if index < 0 {
		index += sqlQuotaBucketCount
	}
	bucket := &state.buckets[index]
	if bucket.queries == 0 && bucket.bytes == 0 && bucket.elapsed == 0 {
		bucket.start = start
	} else if bucket.start != start {
		state.queries -= bucket.queries
		state.bytes -= bucket.bytes
		state.elapsed -= bucket.elapsed
		*bucket = sqlQuotaBucket{start: start}
	}
	return bucket
}

func normalizeSQLQuotaKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "default"
	}
	return key
}

func sqlQuotaShardIndex(key string) int {
	var hash uint64 = 14695981039346656037
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= 1099511628211
	}
	return int(hash & (sqlQuotaShardCount - 1))
}
