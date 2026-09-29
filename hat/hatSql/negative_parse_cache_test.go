package hatSql

import "testing"

func TestSQLPreparedQueryCacheNegativeCachesStableParseErrors(t *testing.T) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT"
	cache := NewSQLPreparedQueryCache(2)

	for i := 0; i < 2; i++ {
		if _, err := ParseSQLQueryWithCache(source, nil, cache); err == nil {
			t.Fatal("invalid source unexpectedly parsed")
		}
	}
	stats := cache.Stats()
	if stats.NegativeEntries != 1 {
		t.Fatalf("negative entries = %d, want 1", stats.NegativeEntries)
	}
	if stats.NegativeAdmissions != 1 {
		t.Fatalf("negative admissions = %d, want 1", stats.NegativeAdmissions)
	}
	if stats.NegativeHits != 1 {
		t.Fatalf("negative hits = %d, want 1", stats.NegativeHits)
	}
	if stats.Entries != 0 || stats.Admissions != 0 {
		t.Fatalf("successful entries = %d admissions = %d, want no successful entry", stats.Entries, stats.Admissions)
	}

	if removed := cache.Invalidate(); removed != 0 {
		t.Fatalf("invalidated successful entries = %d, want 0", removed)
	}
	if stats := cache.Stats(); stats.NegativeEntries != 0 {
		t.Fatalf("negative entries after invalidation = %d, want 0", stats.NegativeEntries)
	}

	uncached := NewSQLPreparedQueryCache(0)
	if _, err := ParseSQLQueryWithCache(source, nil, uncached); err == nil {
		t.Fatal("disabled cache accepted invalid source")
	}
	if stats := uncached.Stats(); stats.NegativeEntries != 0 || stats.NegativeAdmissions != 0 || stats.NegativeHits != 0 {
		t.Fatalf("disabled cache stats = %#v, want no negative cache state", stats)
	}
}

func TestSQLPreparedQueryCacheNegativeSharesCapacity(t *testing.T) {
	const (
		invalidSource = "FROM VALUES (1, 2) AS items(id) SELECT"
		validSource   = "FROM VALUES (1, 2) AS items(id) SELECT id"
	)
	cache := NewSQLPreparedQueryCache(1)
	if _, err := ParseSQLQueryWithCache(invalidSource, nil, cache); err == nil {
		t.Fatal("invalid source unexpectedly parsed")
	}
	if _, err := ParseSQLQueryWithCache(validSource, nil, cache); err != nil {
		t.Fatalf("valid source failed: %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.NegativeEntries != 0 || stats.Evictions != 1 {
		t.Fatalf("after valid admission stats = %#v, want one positive entry and one eviction", stats)
	}
	if _, err := ParseSQLQueryWithCache(invalidSource, nil, cache); err == nil {
		t.Fatal("invalid source unexpectedly parsed after eviction")
	}
	stats = cache.Stats()
	if stats.Entries != 0 || stats.NegativeEntries != 1 || stats.NegativeAdmissions != 2 {
		t.Fatalf("after negative re-admission stats = %#v, want one negative entry", stats)
	}
}
