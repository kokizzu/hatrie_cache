package hatSql

import "testing"

func TestCHG14PreparedQueryCacheReportsAdmissionsAndEvictions(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	if _, err := cache.template("SELECT 1 FROM events"); err != nil {
		t.Fatalf("template(SELECT 1 FROM events) error = %v", err)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Misses != 1 || stats.Admissions != 1 || stats.Evictions != 0 {
		t.Fatalf("after first admission stats = %#v, want one miss/admission and no eviction", stats)
	}

	if _, err := cache.template("SELECT 1 FROM events"); err != nil {
		t.Fatalf("template hit error = %v", err)
	}
	stats = cache.Stats()
	if stats.Hits != 1 || stats.Admissions != 1 || stats.Evictions != 0 {
		t.Fatalf("after hit stats = %#v, want hit without another admission or eviction", stats)
	}

	if _, err := cache.template("SELECT 2 FROM events"); err != nil {
		t.Fatalf("template(SELECT 2 FROM events) error = %v", err)
	}
	stats = cache.Stats()
	if stats.Entries != 1 || stats.Misses != 2 || stats.Admissions != 2 || stats.Evictions != 1 {
		t.Fatalf("after capacity eviction stats = %#v, want two admissions and one eviction", stats)
	}
}

func TestCHG14PreparedQueryCacheDoesNotAdmitParseFailures(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	if _, err := cache.template("SELECT"); err == nil {
		t.Fatal("template(invalid SQL) error = nil, want parse error")
	}
	stats := cache.Stats()
	if stats.Entries != 0 || stats.Misses != 0 || stats.Admissions != 0 || stats.Evictions != 0 {
		t.Fatalf("after parse failure stats = %#v, want no cache counters", stats)
	}
}

func TestCHG14PreparedQueryCacheInvalidationIsNotEviction(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	if _, err := cache.template("SELECT 1 FROM events"); err != nil {
		t.Fatalf("template(SELECT 1 FROM events) error = %v", err)
	}
	if _, err := cache.template("SELECT 2 FROM events"); err != nil {
		t.Fatalf("template(SELECT 2 FROM events) error = %v", err)
	}
	if removed := cache.Invalidate(); removed != 2 {
		t.Fatalf("Invalidate() removed = %d, want 2", removed)
	}
	stats := cache.Stats()
	if stats.Entries != 0 || stats.Admissions != 2 || stats.Evictions != 0 {
		t.Fatalf("after invalidation stats = %#v, want admissions preserved and evictions unchanged", stats)
	}
}
