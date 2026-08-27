package hatCache

import "testing"

func TestSQLJSONBitmapIndexAcceleratesLowCardinalityEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued"},
  {"id":2,"state":"running"},
  {"id":3,"state":"queued"},
  {"id":4,"state":"finished"},
  {"id":5,"state":"queued"}
]`)
	if err := trie.CreateSQLJSONBitmapIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}
	health, ok, err := trie.SQLJSONBitmapIndexHealth("jobs", "state")
	if err != nil || !ok {
		t.Fatalf("SQLJSONBitmapIndexHealth() = %#v, %v, %v", health, ok, err)
	}
	if health.Rows != 5 || health.DistinctKeys != 3 || health.EncodedBytes == 0 || !health.Current {
		t.Fatalf("bitmap index health = %#v", health)
	}
	rows, exact, available, err := trie.SQLJSONIndexValueEstimate("jobs", "state", "queued")
	if err != nil || !available || !exact || rows != 3 {
		t.Fatalf("SQLJSONIndexValueEstimate() = %d, %v, %v, %v", rows, exact, available, err)
	}
	query := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' SELECT job.id ORDER BY job.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["id"] != float64(1) || result.Rows[2]["id"] != float64(5) {
		t.Fatalf("rows = %#v, want queued job ids", result.Rows)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "INDEX SCAN" {
			return
		}
	}
	t.Fatalf("plan = %#v, want INDEX SCAN", explained.Plan)
}
