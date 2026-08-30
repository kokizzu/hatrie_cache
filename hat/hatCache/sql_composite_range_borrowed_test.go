package hatCache

import "testing"

type sqlBorrowedCompositeRangeProbe struct {
	*HatTrie
	borrowedCalls int
}

func (probe *sqlBorrowedCompositeRangeProbe) BorrowSQLCompositeIndexedRangeSource(name, key string, equalityFields []string, equalityValues []interface{}, rangeField, operator string, rangeValue interface{}) ([]SQLRow, bool, error) {
	probe.borrowedCalls++
	return probe.HatTrie.BorrowSQLCompositeIndexedRangeSource(name, key, equalityFields, equalityValues, rangeField, operator, rangeValue)
}

func TestSQLCompositeRangePlannerBorrowsRowsWithoutChangingPublicCopies(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[
  {"id":1,"tenant_id":7,"created_at":10},
  {"id":2,"tenant_id":7,"created_at":20},
  {"id":3,"tenant_id":8,"created_at":30}
]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "events", Fields: []string{"tenant_id", "created_at"}, Type: SQLIndexInt64}); err != nil {
		t.Fatalf("CreateSQLTypedJSONIndex() error = %v", err)
	}
	publicRows, available, err := trie.ResolveSQLCompositeIndexedRangeSource("CACHE", "events", []string{"tenant_id"}, []interface{}{int64(7)}, "created_at", ">=", int64(10))
	if err != nil || !available || len(publicRows) != 2 {
		t.Fatalf("ResolveSQLCompositeIndexedRangeSource() = %#v, %v, %v", publicRows, available, err)
	}
	publicRows[0]["id"] = float64(99)
	publicRows, available, err = trie.ResolveSQLCompositeIndexedRangeSource("CACHE", "events", []string{"tenant_id"}, []interface{}{int64(7)}, "created_at", ">=", int64(10))
	if err != nil || !available || publicRows[0]["id"] != float64(1) {
		t.Fatalf("public resolver lost copy isolation: %#v, %v, %v", publicRows, available, err)
	}
	probe := &sqlBorrowedCompositeRangeProbe{HatTrie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.tenant_id = 7 AND event.created_at >= 10 SELECT event.id", probe)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || probe.borrowedCalls != 1 {
		t.Fatalf("ExecuteSQLQuery() = %#v, %v, borrowed calls = %d", result, err, probe.borrowedCalls)
	}
}
