package hatSql

import (
	"context"
	"errors"
	"sync"

	json "github.com/goccy/go-json"
)

// ResultCache caches query results while the caller-supplied epoch remains
// unchanged. It is portable because the owner supplies both execution and
// invalidation; cache-server adapters can use their mutation epoch directly.
type ResultCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]resultCacheEntry
	order    []string
}

type resultCacheEntry struct {
	epoch  uint64
	result QueryResult
}

// NewResultCache creates a bounded cache. A non-positive capacity disables
// retention while preserving Execute behavior.
func NewResultCache(capacity int) *ResultCache {
	return &ResultCache{capacity: capacity, entries: make(map[string]resultCacheEntry)}
}

// Execute reuses one result only when epoch reports the same value before and
// after the supplied query execution. Returned results never alias the cache.
func (cache *ResultCache) Execute(ctx context.Context, key string, epoch func() uint64, execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	if execute == nil {
		return QueryResult{}, errors.New("hatSql: result cache executor is nil")
	}
	if cache == nil || cache.capacity <= 0 {
		return execute(ctx)
	}
	if epoch == nil {
		return QueryResult{}, errors.New("hatSql: result cache epoch is nil")
	}
	before := epoch()
	cache.mu.Lock()
	entry, ok := cache.entries[key]
	cache.mu.Unlock()
	if ok && entry.epoch == before {
		return cloneResultCacheResult(entry.result)
	}
	result, err := execute(ctx)
	if err != nil || epoch() != before {
		return result, err
	}
	stored, err := cloneResultCacheResult(result)
	if err != nil {
		return result, nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; !exists {
		cache.order = append(cache.order, key)
	}
	cache.entries[key] = resultCacheEntry{epoch: before, result: stored}
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, oldest)
	}
	return result, nil
}

func cloneResultCacheResult(result QueryResult) (QueryResult, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return QueryResult{}, err
	}
	var out QueryResult
	if err := json.Unmarshal(encoded, &out); err != nil {
		return QueryResult{}, err
	}
	return out, nil
}
