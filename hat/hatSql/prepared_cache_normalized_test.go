package hatSql

import (
	"context"
	"testing"
)

func TestSQLPreparedQueryCacheNormalizesEquivalentSQL(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	first := "SELECT value FROM CACHE('users') WHERE id = $1"
	second := "\n select  value\n from cache('users') where id=$1\n"
	if _, err := cache.template(first); err != nil {
		t.Fatalf("first template: %v", err)
	}
	if _, err := cache.template(second); err != nil {
		t.Fatalf("equivalent template: %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("cache stats = %#v, want one normalized entry, one hit, one miss", stats)
	}
}

func TestSQLPreparedQueryCacheSeparatesSchemaVersions(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	source := "SELECT value FROM CACHE('users') WHERE id = $1"
	if _, err := cache.templateWithSchemaVersion(source, "schema-1"); err != nil {
		t.Fatalf("schema-1 template: %v", err)
	}
	if _, err := cache.templateWithSchemaVersion(" select value from cache('users') where id=$1", "schema-1"); err != nil {
		t.Fatalf("normalized schema-1 template: %v", err)
	}
	if _, err := cache.templateWithSchemaVersion(source, "schema-2"); err != nil {
		t.Fatalf("schema-2 template: %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Hits != 1 || stats.Misses != 2 {
		t.Fatalf("versioned cache stats = %#v, want two entries, one hit, two misses", stats)
	}
}

func TestSQLPreparedQueryCacheDoesNotAliasLiteralValues(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	for _, source := range []string{
		"SELECT value FROM CACHE('users') WHERE id = 1",
		"SELECT value FROM CACHE('users') WHERE id = 2",
	} {
		if _, err := cache.template(source); err != nil {
			t.Fatalf("template(%q): %v", source, err)
		}
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Hits != 0 || stats.Misses != 2 {
		t.Fatalf("literal cache stats = %#v, want two distinct misses", stats)
	}
}

func TestPrepareSQLQueryWithSchemaVersionRetainsNamespace(t *testing.T) {
	query, err := PrepareSQLQueryWithSchemaVersion(
		"SELECT value FROM CACHE('users') WHERE id = $1",
		[]ParameterSpec{{Type: ParameterInteger}},
		"schema-7",
		NewSQLPreparedQueryCache(1),
	)
	if err != nil {
		t.Fatalf("PrepareSQLQueryWithSchemaVersion: %v", err)
	}
	if got := query.SchemaVersion(); got != "schema-7" {
		t.Fatalf("SchemaVersion() = %q, want schema-7", got)
	}
}

func TestSQLPreparedQueryCacheNormalizedPlanExecutesEquivalentQuery(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		return []Row{{"id": int64(7), "value": "ready"}}, nil
	})
	first, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('users') WHERE id = $1", resolver, []interface{}{int64(7)}, SQLQueryOptions{PreparedCache: cache})
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), " select value from cache('users') where id=$1", resolver, []interface{}{int64(7)}, SQLQueryOptions{PreparedCache: cache})
	if err != nil {
		t.Fatalf("normalized execution: %v", err)
	}
	if len(first.Rows) != 1 || len(second.Rows) != 1 || first.Rows[0]["value"] != "ready" || second.Rows[0]["value"] != "ready" {
		t.Fatalf("equivalent results = %#v and %#v, want one ready row each", first.Rows, second.Rows)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("execution cache stats = %#v, want one normalized hit", stats)
	}
}

func TestSQLPreparedQueryCacheVersionIsUsedByExecution(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		return []Row{{"id": int64(7), "value": "ready"}}, nil
	})
	source := "SELECT value FROM CACHE('users') WHERE id = $1"
	for _, version := range []string{"schema-1", "schema-2"} {
		result, err := ExecuteSQLQueryParameters(context.Background(), source, resolver, []interface{}{int64(7)}, SQLQueryOptions{
			PreparedCache:         cache,
			PreparedSchemaVersion: version,
		})
		if err != nil {
			t.Fatalf("execution with %s: %v", version, err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["value"] != "ready" {
			t.Fatalf("execution with %s = %#v, want one ready row", version, result.Rows)
		}
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Hits != 0 || stats.Misses != 2 {
		t.Fatalf("execution version stats = %#v, want two versioned plans", stats)
	}
}

func TestSQLPreparedQueryCacheInvalidatesOneSchemaVersion(t *testing.T) {
	cache := NewSQLPreparedQueryCache(4)
	source := "SELECT value FROM CACHE('users') WHERE id = $1"
	for _, version := range []string{"schema-1", "schema-2"} {
		if _, err := cache.templateWithSchemaVersion(source, version); err != nil {
			t.Fatalf("template with %s: %v", version, err)
		}
	}
	if removed := cache.InvalidateSchemaVersion("schema-1"); removed != 1 {
		t.Fatalf("InvalidateSchemaVersion() removed %d plans, want 1", removed)
	}
	if _, err := cache.templateWithSchemaVersion(source, "schema-2"); err != nil {
		t.Fatalf("schema-2 lookup after invalidation: %v", err)
	}
	if _, err := cache.templateWithSchemaVersion(source, "schema-1"); err != nil {
		t.Fatalf("schema-1 rebuild after invalidation: %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Hits != 1 || stats.Misses != 3 {
		t.Fatalf("version invalidation stats = %#v, want one hit and three misses", stats)
	}
}

func TestSQLPreparedQueryCacheInvalidatesAliasesAndKeepsCapacity(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	first := "SELECT value FROM CACHE('users') WHERE id = $1"
	alias := " select value from cache('users') where id=$1"
	if _, err := cache.template(first); err != nil {
		t.Fatalf("first template: %v", err)
	}
	if _, err := cache.template(alias); err != nil {
		t.Fatalf("alias template: %v", err)
	}
	if removed := cache.Invalidate(); removed != 1 {
		t.Fatalf("Invalidate() removed %d plans, want 1", removed)
	}
	if stats := cache.Stats(); stats.Entries != 0 {
		t.Fatalf("cache entries after Invalidate() = %d, want 0", stats.Entries)
	}
	if _, err := cache.template(first); err != nil {
		t.Fatalf("template after full invalidation: %v", err)
	}
	if stats := cache.Stats(); stats.Entries != 1 || stats.Misses != 2 || stats.Hits != 1 {
		t.Fatalf("cache stats after refill = %#v, want one entry, two misses, and one hit", stats)
	}
}
