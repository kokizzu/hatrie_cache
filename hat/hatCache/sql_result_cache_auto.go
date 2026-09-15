package hatCache

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"

	"hatrie_cache/hat/hatSql"
)

const (
	// DefaultSQLResultCacheCapacity is a bounded starting point for callers
	// that want automatic SQL result reuse without choosing a capacity.
	DefaultSQLResultCacheCapacity = 128
)

var ErrSQLResultCacheCapacityInvalid = errors.New("hatriecache: SQL result cache capacity must be non-negative")

type SQLResultCacheStats = hatSql.ResultCacheStats

// ConfigureSQLResultCache enables bounded automatic caching for materialized
// SQL queries executed directly against this trie. Capacity zero disables the
// feature and releases the cache; negative capacities are rejected.
func (ht *HatTrie) ConfigureSQLResultCache(capacity int) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if capacity < 0 {
		return ErrSQLResultCacheCapacityInvalid
	}
	if capacity == 0 {
		ht.sqlResultCache.Store(nil)
		return nil
	}
	ht.sqlResultCache.Store(NewSQLResultCache(capacity))
	return nil
}

// SQLResultCacheStats returns cumulative automatic-cache counters. A trie
// with automatic caching disabled returns the zero value.
func (ht *HatTrie) SQLResultCacheStats() SQLResultCacheStats {
	if ht == nil {
		return SQLResultCacheStats{}
	}
	cache := ht.sqlResultCache.Load()
	if cache == nil || cache.cache == nil {
		return SQLResultCacheStats{}
	}
	return cache.cache.Stats()
}

func (ht *HatTrie) automaticSQLResultCache() *hatSql.ResultCache {
	if ht == nil {
		return nil
	}
	cache := ht.sqlResultCache.Load()
	if cache == nil {
		return nil
	}
	return cache.cache
}

// SQLSourceVersion uses the trie mutation epoch as a conservative source
// version. Any write invalidates all cached queries, including queries over a
// single CACHE key, which keeps partition and resolver behavior correct.
func (ht *HatTrie) SQLSourceVersion(name string, _ string) (string, bool, error) {
	if ht == nil {
		return "", false, ErrNilHatTrie
	}
	if !strings.EqualFold(strings.TrimSpace(name), "CACHE") {
		return "", false, nil
	}
	return strconv.FormatUint(atomic.LoadUint64(&ht.mutationEpoch), 10), true, nil
}
