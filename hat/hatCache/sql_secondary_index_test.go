package hatCache

import "testing"

func TestSQLBitmapSecondaryIndexIntersectionAndUnion(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued","priority":"high"},
  {"id":2,"state":"queued","priority":"low"},
  {"id":3,"state":"running","priority":"high"},
  {"id":4,"state":"queued","priority":"high"},
  {"id":5,"state":"finished","priority":"low"}
]`)
	for _, field := range []string{"state", "priority"} {
		if err := trie.CreateSQLJSONBitmapIndex("jobs", field); err != nil {
			t.Fatalf("CreateSQLJSONBitmapIndex(%q) error = %v", field, err)
		}
	}

	intersection := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' AND job.priority = 'high' SELECT job.id ORDER BY job.id"
	result, err := ExecuteSQLQuery(intersection, trie)
	if err != nil {
		t.Fatalf("intersection query error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["id"] != float64(4) {
		t.Fatalf("intersection rows = %#v, want ids 1 and 4", result.Rows)
	}
	assertSQLSecondaryIndexPlan(t, "INDEX INTERSECTION", intersection, trie)

	union := "FROM CACHE('jobs') AS job WHERE job.state = 'running' OR job.priority = 'high' SELECT job.id ORDER BY job.id"
	result, err = ExecuteSQLQuery(union, trie)
	if err != nil {
		t.Fatalf("union query error = %v", err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["id"] != float64(3) || result.Rows[2]["id"] != float64(4) {
		t.Fatalf("union rows = %#v, want ids 1, 3, and 4", result.Rows)
	}
	assertSQLSecondaryIndexPlan(t, "INDEX UNION", union, trie)
}

func assertSQLSecondaryIndexPlan(t *testing.T, node, query string, trie *HatTrie) {
	t.Helper()
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == node {
			return
		}
	}
	t.Fatalf("plan = %#v, want %s", explained.Plan, node)
}
