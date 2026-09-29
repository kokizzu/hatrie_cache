package hatSql

import "encoding/binary"

type sqlPreparedQueryCacheLookupKey struct {
	source        string
	schemaVersion string
}

// templateWithSchemaVersion returns an immutable parsed template keyed by the
// normalized token stream and caller-provided schema version. Literal values
// remain in the key because this cache stores parsed ASTs, not parameterized
// expression nodes.
func (cache *SQLPreparedQueryCache) templateWithSchemaVersion(source, schemaVersion string) (*sqlQuery, error) {
	if cache == nil || cache.capacity <= 0 {
		return parseSQLQueryTemplate(source)
	}
	lookupKey := sqlPreparedQueryCacheLookupKey{source: source, schemaVersion: schemaVersion}
	var entry sqlPreparedQueryCacheEntry
	cache.mu.Lock()
	if schemaVersion == "" {
		if entry, ok := cache.exactEntries[source]; ok {
			cache.hits++
			cache.order.MoveToBack(entry.order)
			cache.mu.Unlock()
			return entry.query, nil
		}
	} else if entry, ok := cache.versionedExactEntries[lookupKey]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		cache.mu.Unlock()
		return entry.query, nil
	}
	if entry, ok := cache.negativeEntries[lookupKey]; ok {
		cache.negativeHits++
		cache.order.MoveToBack(entry.order)
		err := entry.err
		cache.mu.Unlock()
		return nil, err
	}
	cache.mu.Unlock()
	key, err := sqlPreparedQueryCacheKey(source, schemaVersion)
	if err != nil {
		cache.mu.Lock()
		if entry, ok := cache.negativeEntries[lookupKey]; ok {
			cache.negativeHits++
			error := entry.err
			cache.order.MoveToBack(entry.order)
			cache.mu.Unlock()
			return nil, error
		}
		sqlPreparedQueryCacheRememberNegative(cache, lookupKey, err)
		cache.mu.Unlock()
		return nil, err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := sqlPreparedQueryCacheExactEntry(cache, lookupKey); ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		return entry.query, nil
	}
	if entry, ok := cache.entries[key]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		sqlPreparedQueryCacheDeleteExactEntry(cache, entry.lookupKey)
		entry.lookupKey = lookupKey
		cache.entries[key] = entry
		sqlPreparedQueryCacheSetExactEntry(cache, lookupKey, entry)
		return entry.query, nil
	}
	query, err := parseSQLQueryTemplate(source)
	if err != nil {
		sqlPreparedQueryCacheRememberNegative(cache, lookupKey, err)
		return nil, err
	}
	cache.misses++
	sqlPreparedQueryCacheMakeRoom(cache)
	entry = sqlPreparedQueryCacheEntry{query: query, lookupKey: lookupKey}
	entry.order = cache.order.PushBack(sqlPreparedQueryCacheOrderEntry{key: key, lookupKey: lookupKey})
	cache.entries[key] = entry
	sqlPreparedQueryCacheSetExactEntry(cache, lookupKey, entry)
	cache.admissions++
	return query, nil
}

func sqlPreparedQueryCacheExactEntry(cache *SQLPreparedQueryCache, lookupKey sqlPreparedQueryCacheLookupKey) (sqlPreparedQueryCacheEntry, bool) {
	if lookupKey.schemaVersion == "" {
		entry, ok := cache.exactEntries[lookupKey.source]
		return entry, ok
	}
	entry, ok := cache.versionedExactEntries[lookupKey]
	return entry, ok
}

func sqlPreparedQueryCacheSetExactEntry(cache *SQLPreparedQueryCache, lookupKey sqlPreparedQueryCacheLookupKey, entry sqlPreparedQueryCacheEntry) {
	if lookupKey.schemaVersion == "" {
		cache.exactEntries[lookupKey.source] = entry
		return
	}
	cache.versionedExactEntries[lookupKey] = entry
}

func sqlPreparedQueryCacheDeleteExactEntry(cache *SQLPreparedQueryCache, lookupKey sqlPreparedQueryCacheLookupKey) {
	if lookupKey.schemaVersion == "" {
		delete(cache.exactEntries, lookupKey.source)
		return
	}
	delete(cache.versionedExactEntries, lookupKey)
}

func sqlPreparedQueryCacheMakeRoom(cache *SQLPreparedQueryCache) {
	for len(cache.entries)+len(cache.negativeEntries) >= cache.capacity {
		oldest := cache.order.Front()
		if oldest == nil {
			return
		}
		orderEntry := oldest.Value.(sqlPreparedQueryCacheOrderEntry)
		if orderEntry.negative {
			delete(cache.negativeEntries, orderEntry.lookupKey)
		} else if entry, ok := cache.entries[orderEntry.key]; ok {
			delete(cache.entries, orderEntry.key)
			sqlPreparedQueryCacheDeleteExactEntry(cache, entry.lookupKey)
		}
		cache.order.Remove(oldest)
		cache.evictions++
	}
}

func sqlPreparedQueryCacheRememberNegative(cache *SQLPreparedQueryCache, lookupKey sqlPreparedQueryCacheLookupKey, err error) {
	if cache.negativeEntries == nil {
		cache.negativeEntries = map[sqlPreparedQueryCacheLookupKey]sqlPreparedQueryCacheNegativeEntry{}
	}
	if entry, ok := cache.negativeEntries[lookupKey]; ok {
		cache.order.MoveToBack(entry.order)
		return
	}
	sqlPreparedQueryCacheMakeRoom(cache)
	entry := sqlPreparedQueryCacheNegativeEntry{err: err}
	entry.order = cache.order.PushBack(sqlPreparedQueryCacheOrderEntry{lookupKey: lookupKey, negative: true})
	cache.negativeEntries[lookupKey] = entry
	cache.negativeAdmissions++
}

// Invalidate removes every parsed template, remembered source alias, and
// negative parse result while retaining the cache capacity and counters. It is
// useful after a broad index or projection rebuild.
func (cache *SQLPreparedQueryCache) Invalidate() int {
	return cache.invalidatePreparedPlans(false, "")
}

// InvalidateSchemaVersion removes parsed templates in one schema-version
// namespace. The empty version is a valid namespace and is not treated as a
// request to clear every entry.
func (cache *SQLPreparedQueryCache) InvalidateSchemaVersion(schemaVersion string) int {
	return cache.invalidatePreparedPlans(true, schemaVersion)
}

func (cache *SQLPreparedQueryCache) invalidatePreparedPlans(scoped bool, schemaVersion string) int {
	if cache == nil {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	removed := 0
	for key, entry := range cache.entries {
		if scoped && entry.lookupKey.schemaVersion != schemaVersion {
			continue
		}
		cache.order.Remove(entry.order)
		delete(cache.entries, key)
		sqlPreparedQueryCacheDeleteExactEntry(cache, entry.lookupKey)
		removed++
	}
	for key, entry := range cache.negativeEntries {
		if scoped && key.schemaVersion != schemaVersion {
			continue
		}
		cache.order.Remove(entry.order)
		delete(cache.negativeEntries, key)
	}
	return removed
}

// sqlPreparedQueryCacheKey encodes parser tokens instead of source bytes, so
// whitespace and keyword casing do not create duplicate plans. Token kinds and
// length prefixes make the key unambiguous without relying on a hash.
func sqlPreparedQueryCacheKey(source, schemaVersion string) (string, error) {
	tokens, err := Lex(source)
	if err != nil {
		return "", err
	}
	key := make([]byte, 0, len(source)+len(schemaVersion)+16)
	key = appendSQLPreparedQueryCachePart(key, schemaVersion)
	for _, token := range tokens {
		if token.Kind() == TokenEOF {
			break
		}
		key = append(key, byte(token.Kind()))
		key = appendSQLPreparedQueryCachePart(key, formatSQLToken(token))
	}
	return string(key), nil
}

func appendSQLPreparedQueryCachePart(destination []byte, value string) []byte {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(value)))
	destination = append(destination, length[:n]...)
	return append(destination, value...)
}
