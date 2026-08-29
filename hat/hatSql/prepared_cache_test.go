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
