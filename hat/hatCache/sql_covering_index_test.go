package hatCache

import "testing"

func TestSQLJSONCoveringIndexRefreshesAfterStringReplacement(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONCoveringIndex("jobs", "state", "id"); err != nil {
		t.Fatal(err)
	}
	before, available, err := trie.ResolveSQLCoveringSource("CACHE", "jobs", "state", "queued", []string{"id", "state"})
	if err != nil || !available || len(before) != 1 || before[0]["id"] != float64(1) {
		t.Fatalf("initial ResolveSQLCoveringSource() = %#v, %v, %v", before, available, err)
	}
	trie.UpsertString("jobs", `[{"id":2,"state":"running"}]`)
	after, available, err := trie.ResolveSQLCoveringSource("CACHE", "jobs", "state", "running", []string{"id", "state"})
	if err != nil || !available || len(after) != 1 || after[0]["id"] != float64(2) {
		t.Fatalf("refreshed ResolveSQLCoveringSource() = %#v, %v, %v", after, available, err)
	}
}

func TestSQLJSONCoveringIndexProjectsWithoutSourceRows(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued","name":"first","payload":"not-covered"},
  {"id":2,"state":"running","name":"second","payload":"not-covered"},
  {"id":3,"state":"queued","name":"third","payload":"not-covered"}
]`)
	if err := trie.CreateSQLJSONCoveringIndex("jobs", "state", "id", "name"); err != nil {
		t.Fatalf("CreateSQLJSONCoveringIndex() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLCoveringSource("CACHE", "jobs", "state", "queued", []string{"id", "name", "state"})
	if err != nil || !available {
		t.Fatalf("ResolveSQLCoveringSource() = %#v, %v, %v", rows, available, err)
	}
	if len(rows) != 2 || rows[0]["id"] != float64(1) || rows[1]["name"] != "third" {
		t.Fatalf("covering rows = %#v", rows)
	}
	if _, found := rows[0]["payload"]; found {
		t.Fatalf("covering row unexpectedly retained payload: %#v", rows[0])
	}
	query := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' SELECT job.id, job.name"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["name"] != "third" {
		t.Fatalf("rows = %#v, want covered queued jobs", result.Rows)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COVERING INDEX SCAN" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COVERING INDEX SCAN", explained.Plan)
}
