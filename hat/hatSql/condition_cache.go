package hatSql

import (
	"container/list"
	"sync"
)

// SQLQueryConditionCacheStats reports bounded condition-selection reuse.
type SQLQueryConditionCacheStats struct {
	Entries int
	Hits    uint64
	Misses  uint64
}

// QueryConditionCacheStats is the package-native name for condition-cache statistics.
type QueryConditionCacheStats = SQLQueryConditionCacheStats

// SQLQueryConditionCache stores immutable columnar match positions by source
// version. It is safe for concurrent use. A nil cache, nonpositive capacity,
// or nonpositive maximum matched rows disables storage.
type SQLQueryConditionCache struct {
	mu             sync.Mutex
	capacity       int
	maxMatchedRows int
	entries        map[sqlQueryConditionCacheKey]sqlQueryConditionCacheEntry
	order          *list.List
	hits           uint64
	misses         uint64
}

type sqlQueryConditionCacheKey struct {
	sourceKind string
	sourceKey  string
	version    string
	predicate  string
	collation  SQLCollation
	rows       int
}

type sqlQueryConditionCacheEntry struct {
	matches []int
	order   *list.Element
}

// QueryConditionCache is the package-native name for SQLQueryConditionCache.
type QueryConditionCache = SQLQueryConditionCache

// NewSQLQueryConditionCache creates a bounded source-versioned selection
// cache. maximumMatchedRows is a hard per-entry bound; it prevents a broad
// predicate from retaining an unbounded row-position vector.
func NewSQLQueryConditionCache(capacity, maximumMatchedRows int) *SQLQueryConditionCache {
	return &SQLQueryConditionCache{
		capacity:       capacity,
		maxMatchedRows: maximumMatchedRows,
		entries:        map[sqlQueryConditionCacheKey]sqlQueryConditionCacheEntry{},
		order:          list.New(),
	}
}

// NewQueryConditionCache creates a bounded source-versioned selection cache.
func NewQueryConditionCache(capacity, maximumMatchedRows int) *QueryConditionCache {
	return NewSQLQueryConditionCache(capacity, maximumMatchedRows)
}

// Stats returns a stable snapshot of cache counters.
func (cache *SQLQueryConditionCache) Stats() SQLQueryConditionCacheStats {
	if cache == nil {
		return SQLQueryConditionCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return SQLQueryConditionCacheStats{Entries: len(cache.entries), Hits: cache.hits, Misses: cache.misses}
}

func (cache *SQLQueryConditionCache) enabled() bool {
	return cache != nil && cache.capacity > 0 && cache.maxMatchedRows > 0
}

func (cache *SQLQueryConditionCache) get(key sqlQueryConditionCacheKey) ([]int, bool) {
	if !cache.enabled() {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.entries[key]
	if !ok {
		cache.misses++
		return nil, false
	}
	cache.order.MoveToFront(entry.order)
	cache.hits++
	return entry.matches, true
}

func (cache *SQLQueryConditionCache) put(key sqlQueryConditionCacheKey, matches []int) {
	if !cache.enabled() || len(matches) > cache.maxMatchedRows {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok {
		entry.matches = append(entry.matches[:0], matches...)
		cache.entries[key] = entry
		cache.order.MoveToFront(entry.order)
		return
	}
	for len(cache.entries) >= cache.capacity {
		oldest := cache.order.Back()
		if oldest == nil {
			break
		}
		delete(cache.entries, oldest.Value.(sqlQueryConditionCacheKey))
		cache.order.Remove(oldest)
	}
	cache.entries[key] = sqlQueryConditionCacheEntry{
		matches: append([]int(nil), matches...),
		order:   cache.order.PushFront(key),
	}
}
