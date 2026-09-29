package hatSql

import "testing"

func TestSQLCompiledQueryCacheNegativeCachesStableCompileErrors(t *testing.T) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT"
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cache.Compile(source); err == nil {
			t.Fatal("invalid source unexpectedly compiled")
		}
	}
	stats := cache.Stats()
	if stats.NegativeEntries != 1 || stats.NegativeAdmissions != 1 || stats.NegativeHits != 1 {
		t.Fatalf("negative stats = %#v, want one admission and one hit", stats)
	}
	if stats.Entries != 0 {
		t.Fatalf("successful entries = %d, want 0", stats.Entries)
	}
	if removed := cache.Invalidate(); removed != 0 {
		t.Fatalf("invalidated successful entries = %d, want 0", removed)
	}
	if stats := cache.Stats(); stats.NegativeEntries != 0 || stats.Bytes != 0 {
		t.Fatalf("after invalidation stats = %#v, want no negative entry", stats)
	}
	if _, err := cache.Compile(source); err == nil {
		t.Fatal("invalid source unexpectedly compiled after invalidation")
	}
	if stats := cache.Stats(); stats.NegativeAdmissions != 2 {
		t.Fatalf("negative admissions after invalidation = %d, want 2", stats.NegativeAdmissions)
	}
}

func TestSQLCompiledQueryCacheNegativeSharesCapacity(t *testing.T) {
	const (
		invalidSource = "FROM VALUES (1, 2) AS items(id) SELECT"
		validSource   = "FROM VALUES (1, 2) AS items(id) SELECT id"
	)
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Compile(invalidSource); err == nil {
		t.Fatal("invalid source unexpectedly compiled")
	}
	if _, err := cache.Compile(validSource); err != nil {
		t.Fatalf("valid source failed: %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.NegativeEntries != 0 || stats.Evictions != 1 {
		t.Fatalf("after valid admission stats = %#v, want one positive entry and one eviction", stats)
	}
	if _, err := cache.Compile(invalidSource); err == nil {
		t.Fatal("invalid source unexpectedly compiled after eviction")
	}
	stats = cache.Stats()
	if stats.Entries != 0 || stats.NegativeEntries != 1 || stats.NegativeAdmissions != 2 {
		t.Fatalf("after negative re-admission stats = %#v, want one negative entry", stats)
	}
}

func TestSQLCompiledQueryCacheNegativeHonorsByteLimit(t *testing.T) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT"
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 4, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Compile(source); err == nil {
		t.Fatal("invalid source unexpectedly compiled")
	}
	stats := cache.Stats()
	if stats.NegativeEntries != 0 || stats.NegativeAdmissions != 0 || stats.Bytes != 0 {
		t.Fatalf("oversized negative stats = %#v, want no admission", stats)
	}
}
