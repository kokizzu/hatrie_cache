package hatSql

import "testing"

func TestSQLPreparedQueryCacheEvictsLeastRecentlyUsedTemplate(t *testing.T) {
	cache := NewSQLPreparedQueryCache(2)
	for _, source := range []string{"SELECT 1 FROM CACHE('one')", "SELECT 2 FROM CACHE('two')", "SELECT 1 FROM CACHE('one')", "SELECT 3 FROM CACHE('three')"} {
		if _, err := cache.template(source); err != nil {
			t.Fatalf("template(%q) error = %v", source, err)
		}
	}
	before := cache.Stats()
	if _, err := cache.template("SELECT 2 FROM CACHE('two')"); err != nil {
		t.Fatalf("template(evicted) error = %v", err)
	}
	after := cache.Stats()
	if after.Entries != 2 || after.Misses != before.Misses+1 {
		t.Fatalf("cache stats after evicted lookup = %#v, want one miss with two entries", after)
	}
}

func TestSQLPreparedQueryCacheReportsAdmissionAndMemory(t *testing.T) {
	cache := NewSQLPreparedQueryCache(1)
	if _, err := cache.template("SELECT 1 FROM CACHE('one')"); err != nil {
		t.Fatalf("first template: %v", err)
	}
	first := cache.Stats()
	if first.Capacity != 1 || first.Entries != 1 || first.EstimatedBytes <= 0 {
		t.Fatalf("first cache stats = %#v, want capacity=1, one entry, and retained bytes", first)
	}

	if _, err := cache.template("SELECT 2 FROM CACHE('two')"); err != nil {
		t.Fatalf("second template: %v", err)
	}
	second := cache.Stats()
	if second.Capacity != 1 || second.Entries != 1 || second.Misses != 2 || second.Evictions != 1 || second.EstimatedBytes <= 0 {
		t.Fatalf("eviction cache stats = %#v, want one entry, two misses, one eviction, and retained bytes", second)
	}

	if removed := cache.Invalidate(); removed != 1 {
		t.Fatalf("Invalidate() removed %d entries, want 1", removed)
	}
	cleared := cache.Stats()
	if cleared.Entries != 0 || cleared.EstimatedBytes != 0 || cleared.Evictions != 1 {
		t.Fatalf("cleared cache stats = %#v, want no entries/bytes and preserved eviction count", cleared)
	}

	disabled := NewSQLPreparedQueryCache(0)
	if _, err := disabled.template("SELECT 3 FROM CACHE('three')"); err != nil {
		t.Fatalf("disabled template: %v", err)
	}
	if stats := disabled.Stats(); stats.Capacity != 0 || stats.Entries != 0 || stats.EstimatedBytes != 0 || stats.Evictions != 0 {
		t.Fatalf("disabled cache stats = %#v, want zero storage counters", stats)
	}
}
