package hatSql

import "testing"

func TestSQLPreparedQueryCacheReportsEvictions(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	queries := []string{
		"FROM VALUES (1) AS items(id) SELECT id",
		"FROM VALUES (1) AS items(id) SELECT id, id",
	}
	for _, source := range queries {
		if _, err := ParseSQLQueryWithCache(source, nil, cache); err != nil {
			t.Fatalf("ParseSQLQueryWithCache(%q): %v", source, err)
		}
	}

	stats := cache.Stats()
	if stats.Misses != 2 {
		t.Fatalf("misses = %d, want 2", stats.Misses)
	}
	if stats.Admissions != 2 {
		t.Fatalf("admissions = %d, want 2", stats.Admissions)
	}
	if stats.Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", stats.Evictions)
	}
	if stats.Entries != 1 {
		t.Fatalf("entries = %d, want 1", stats.Entries)
	}
}

func TestSQLPreparedQueryCacheAdmissionMetricsIgnoreHitsAndDisabledCaches(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	const source = "FROM VALUES (1) AS items(id) SELECT id"
	if _, err := ParseSQLQueryWithCache(source, nil, cache); err != nil {
		t.Fatalf("first ParseSQLQueryWithCache(): %v", err)
	}
	if _, err := ParseSQLQueryWithCache(source, nil, cache); err != nil {
		t.Fatalf("second ParseSQLQueryWithCache(): %v", err)
	}
	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Admissions != 1 || stats.Evictions != 0 {
		t.Fatalf("hit metrics = %#v, want one hit/miss/admission and no eviction", stats)
	}

	disabled := NewSQLPreparedQueryCache(0)
	if _, err := ParseSQLQueryWithCache(source, nil, disabled); err != nil {
		t.Fatalf("disabled ParseSQLQueryWithCache(): %v", err)
	}
	if stats := disabled.Stats(); stats != (SQLPreparedQueryCacheStats{}) {
		t.Fatalf("disabled metrics = %#v, want zero snapshot", stats)
	}
}
