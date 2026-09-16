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

var (
	ErrSQLResultCacheCapacityInvalid = errors.New("hatriecache: SQL result cache capacity must be non-negative")
	ErrSQLResultCacheDisabled        = errors.New("hatriecache: automatic SQL result cache is disabled")
)

type SQLResultCacheStats = hatSql.ResultCacheStats
type SQLResultCachePersistenceOptions = hatSql.SQLResultCachePersistenceOptions

var (
	ErrSQLResultCachePersistenceCorrupt  = hatSql.ErrSQLResultCachePersistenceCorrupt
	ErrSQLResultCachePersistenceTooLarge = hatSql.ErrSQLResultCachePersistenceTooLarge
)

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

// PersistSQLResultCache atomically saves the enabled automatic SQL result
// cache. Automatic caching remains disabled unless ConfigureSQLResultCache
// was called with a positive capacity.
func (ht *HatTrie) PersistSQLResultCache(path string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	cache := ht.automaticSQLResultCache()
	if cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.Persist(path)
}

// RestoreSQLResultCache loads a previously persisted automatic SQL result
// cache. Missing files are treated as a cold start by the hatSql layer.
func (ht *HatTrie) RestoreSQLResultCache(path string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	cache := ht.automaticSQLResultCache()
	if cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.Restore(path)
}

// PersistSQLResultCacheWithOptions saves the automatic cache with an explicit
// file-size quota.
func (ht *HatTrie) PersistSQLResultCacheWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	cache := ht.automaticSQLResultCache()
	if cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.PersistWithOptions(path, options)
}

// RestoreSQLResultCacheWithOptions restores the automatic cache with an
// explicit file-size quota.
func (ht *HatTrie) RestoreSQLResultCacheWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	cache := ht.automaticSQLResultCache()
	if cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.RestoreWithOptions(path, options)
}

// Persist writes an explicitly constructed SQLResultCache to path.
func (cache *SQLResultCache) Persist(path string) error {
	if cache == nil || cache.cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.cache.Persist(path)
}

// Restore loads an explicitly constructed SQLResultCache from path.
func (cache *SQLResultCache) Restore(path string) error {
	if cache == nil || cache.cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.cache.Restore(path)
}

// PersistWithOptions writes an explicitly constructed SQLResultCache with an
// explicit file-size quota.
func (cache *SQLResultCache) PersistWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if cache == nil || cache.cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.cache.PersistWithOptions(path, options)
}

// RestoreWithOptions loads an explicitly constructed SQLResultCache with an
// explicit file-size quota.
func (cache *SQLResultCache) RestoreWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if cache == nil || cache.cache == nil {
		return ErrSQLResultCacheDisabled
	}
	return cache.cache.RestoreWithOptions(path, options)
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
