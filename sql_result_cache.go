package hatriecache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	json "github.com/goccy/go-json"
)

// SQLResultCache caches default-option read-query results. A result is used
// only while the source trie remains at the mutation epoch it was computed at.
type SQLResultCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]sqlResultCacheEntry
	order    []string
}

type sqlResultCacheEntry struct {
	epoch  uint64
	result SQLQueryResult
}

// NewSQLResultCache creates a bounded cache. A non-positive capacity disables
// retention while preserving the Execute API.
func NewSQLResultCache(capacity int) *SQLResultCache {
	return &SQLResultCache{capacity: capacity, entries: make(map[string]sqlResultCacheEntry)}
}

// Execute runs a default-option read query and returns a cached result only if
// the trie has not been mutated since that result was stored.
func (cache *SQLResultCache) Execute(ctx context.Context, trie *HatTrie, source string, parameters []interface{}) (SQLQueryResult, error) {
	if trie == nil {
		return SQLQueryResult{}, ErrNilHatTrie
	}
	key, err := sqlResultCacheKey(source, parameters)
	if err != nil {
		return SQLQueryResult{}, err
	}
	epoch := atomic.LoadUint64(&trie.mutationEpoch)
	if cache != nil && cache.capacity > 0 {
		cache.mu.Lock()
		entry, ok := cache.entries[key]
		cache.mu.Unlock()
		if ok && entry.epoch == epoch {
			return cloneSQLResultCacheResult(entry.result)
		}
	}
	result, err := ExecuteSQLQueryParameters(ctx, source, trie, parameters, SQLQueryOptions{})
	if err != nil || cache == nil || cache.capacity <= 0 {
		return result, err
	}
	if atomic.LoadUint64(&trie.mutationEpoch) != epoch {
		return result, nil
	}
	stored, err := cloneSQLResultCacheResult(result)
	if err != nil {
		return result, nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; !exists {
		cache.order = append(cache.order, key)
	}
	cache.entries[key] = sqlResultCacheEntry{epoch: epoch, result: stored}
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, oldest)
	}
	return result, nil
}

func sqlResultCacheKey(source string, parameters []interface{}) (string, error) {
	encoded, err := json.Marshal(parameters)
	if err != nil {
		return "", errors.New("hatriecache: SQL result cache parameters are not serializable")
	}
	return source + "\x00" + string(encoded), nil
}

func cloneSQLResultCacheResult(result SQLQueryResult) (SQLQueryResult, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return SQLQueryResult{}, err
	}
	var out SQLQueryResult
	if err := json.Unmarshal(encoded, &out); err != nil {
		return SQLQueryResult{}, err
	}
	return out, nil
}
