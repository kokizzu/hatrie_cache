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

func TestSQLColumnarScanUsesNumericVectorFilter(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("metrics", `[
  {"id":1,"value":10,"payload":"unselected payload one"},
  {"id":2,"value":20,"payload":"unselected payload two"},
  {"id":3,"value":30,"payload":"unselected payload three"}
]`)
	query := "FROM CACHE('metrics') AS metric WHERE metric.value >= 20 SELECT metric.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if want := []SQLRow{{"id": float64(2)}, {"id": float64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("columnar rows = %#v, want %#v", result.Rows, want)
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
		if step.Node == "COLUMNAR NUMERIC FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR NUMERIC FILTER", explained.Plan)
}

func TestSQLColumnarBatchDictionaryEncodesRepeatedStrings(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("states", `[
  {"id":1,"state":"queued"},
  {"id":2,"state":"running"},
  {"id":3,"state":"queued"},
  {"id":4,"state":"queued"},
  {"id":5,"state":"running"},
  {"id":6,"state":"queued"}
]`)
	batch, available, err := trie.ResolveSQLColumnarSource("CACHE", "states", []string{"state", "id"})
	if err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() = %#v, %v, %v", batch, available, err)
	}
	dictionary, encoded := batch.Dictionaries["state"]
	if !encoded || !reflect.DeepEqual(dictionary.Values, []string{"queued", "running"}) || !reflect.DeepEqual(dictionary.Codes, []uint32{0, 1, 0, 0, 1, 0}) {
		t.Fatalf("state dictionary = %#v, encoded=%v", dictionary, encoded)
	}
	if _, retained := batch.Columns["state"]; retained {
		t.Fatalf("dictionary state still retained a row value slice: %#v", batch.Columns)
	}

	query := "FROM CACHE('states') AS state WHERE state.state = 'queued' SELECT state.id"
	columnar, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("dictionary columnar result = %#v, materialized = %#v", columnar, materialized)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR DICTIONARY FILTER" {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY FILTER", explained.Plan)
}
