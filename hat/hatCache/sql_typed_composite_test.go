package hatCache

import "testing"

func TestSQLTypedInt64CompositeIndexAcceleratesEqualityPrefixRange(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[
  {"id":1,"tenant_id":1,"created_at":30},
  {"id":2,"tenant_id":2,"created_at":10},
  {"id":3,"tenant_id":2,"created_at":20},
  {"id":4,"tenant_id":2,"created_at":30},
  {"id":5,"tenant_id":3,"created_at":40}
]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
		CacheKey: "events",
		Fields:   []string{"tenant_id", "created_at"},
		Type:     SQLIndexInt64,
	}); err != nil {
		t.Fatalf("CreateSQLTypedJSONIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.tenant_id = 2 AND event.created_at >= 20 SELECT event.id ORDER BY event.created_at", trie)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != float64(3) || result.Rows[1]["id"] != float64(4) {
		t.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
	}
	trie.sqlIndexMu.RLock()
	index := trie.sqlJSONTypedInt64CompositeIndexes["events"][sqlJSONCompositeIndexIdentifier([]string{"tenant_id", "created_at"})]
	trie.sqlIndexMu.RUnlock()
	if index == nil || index.raw == "" {
		t.Fatal("planner did not build the typed composite index")
	}
	stats, available, err := trie.SQLJSONIndexStats("events", "tenant_id", "created_at")
	if err != nil || !available || stats.Rows != 5 || stats.DistinctKeys != 5 || stats.NullRows != 0 {
		t.Fatalf("SQLJSONIndexStats() = %#v, %v, %v", stats, available, err)
	}
	rows, available, err := trie.ResolveSQLCompositeIndexedRangeSource("CACHE", "events", []string{"tenant_id"}, []interface{}{int64(2)}, "created_at", ">=", int64(20))
	if err != nil || !available || len(rows) != 2 || rows[0]["id"] != float64(3) || rows[1]["id"] != float64(4) {
		t.Fatalf("ResolveSQLCompositeIndexedRangeSource() = %#v, %v, %v", rows, available, err)
	}
	trie.UpsertString("events", `[
  {"id":1,"tenant_id":1,"created_at":30},
  {"id":2,"tenant_id":2,"created_at":10},
  {"id":3,"tenant_id":2,"created_at":20},
  {"id":4,"tenant_id":2,"created_at":30},
  {"id":5,"tenant_id":2,"created_at":50}
]`)
	if err := trie.ScheduleSQLJSONIndexRebuild("events", "created_at"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}
	if rebuilt, err := trie.RunScheduledSQLJSONIndexRebuilds(1); err != nil || rebuilt != 1 {
		t.Fatalf("RunScheduledSQLJSONIndexRebuilds() = %d, %v", rebuilt, err)
	}
	rows, available, err = trie.ResolveSQLCompositeIndexedRangeSource("CACHE", "events", []string{"tenant_id"}, []interface{}{int64(2)}, "created_at", ">=", int64(20))
	if err != nil || !available || len(rows) != 3 || rows[2]["id"] != float64(5) {
		t.Fatalf("refreshed ResolveSQLCompositeIndexedRangeSource() = %#v, %v, %v", rows, available, err)
	}
}
