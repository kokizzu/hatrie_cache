package hatCache

import (
	"reflect"
	"testing"
)

type sqlRowsOnlyResolver struct{ trie *HatTrie }

func (resolver sqlRowsOnlyResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func TestSQLColumnarScanFiltersBeforeProjectionMaterialization(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"queued","name":"first","payload":"unselected payload one"},
  {"id":2,"state":"running","name":"second","payload":"unselected payload two"},
  {"id":3,"state":"queued","name":"third","payload":"unselected payload three"}
]`)

	batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"state", "id", "name"})
	if err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() = %#v, %v, %v", batch, available, err)
	}
	if batch.Rows != 3 || len(batch.Columns) != 3 || batch.Columns["state"][1] != "running" || batch.Columns["id"][2] != float64(3) {
		t.Fatalf("columnar batch = %#v", batch)
	}
	if _, found := batch.Columns["payload"]; found {
		t.Fatalf("columnar batch unexpectedly retained unrequested payload: %#v", batch.Columns)
	}

	query := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' SELECT job.id, job.name"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["name"] != "third" {
		t.Fatalf("rows = %#v, want queued projected jobs", result.Rows)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("materialized ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(result, materialized) {
		t.Fatalf("columnar result = %#v, materialized result = %#v", result, materialized)
	}

	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR SCAN" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR SCAN", explained.Plan)
}
