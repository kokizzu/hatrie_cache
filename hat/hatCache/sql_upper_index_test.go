package hatCache

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONUpperIndexUsesEqualityAndINQueries(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"name":"Ada"},
  {"id":2,"name":"ADA"},
  {"id":3,"name":"Grace"},
  {"id":4,"name":null}
]`)
	if err := trie.CreateSQLJSONUpperIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONUpperIndex() error = %v", err)
	}

	rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", hatSql.UpperIndexField("name"), "ADA")
	if err != nil || !available || len(rows) != 2 {
		t.Fatalf("direct upper lookup = %#v, available %t, error %v", rows, available, err)
	}
	consistency, available, err := trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available || !consistency.Consistent || len(consistency.Indexes) != 1 || consistency.Indexes[0].Kind != "upper" {
		t.Fatalf("upper index consistency = %#v, available %t, error %v", consistency, available, err)
	}

	query := "FROM CACHE('people') AS person WHERE UPPER(person.name) IN ('ADA', 'GRACE', 'ADA') SELECT person.id ORDER BY person.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("indexed UPPER query error = %v", err)
	}
	want := SQLQueryResult{Rows: []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(3)}}}
	if !reflect.DeepEqual(result.Rows, want.Rows) {
		t.Fatalf("indexed UPPER rows = %#v, want %#v", result.Rows, want.Rows)
	}
}

func TestSQLJSONUpperIndexRefreshesAndFallsBackForMixedTypes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"Ada"},{"id":2,"name":"Grace"}]`)
	if err := trie.CreateSQLJSONUpperIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONUpperIndex() error = %v", err)
	}

	query := "FROM CACHE('people') AS person WHERE UPPER(person.name) = 'ADA' SELECT person.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("initial UPPER query = %#v, error %v", result, err)
	}

	trie.UpsertString("people", `[{"id":3,"name":"AdA"}]`)
	result, err = ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(3) {
		t.Fatalf("refreshed UPPER query = %#v, error %v", result, err)
	}

	trie.UpsertString("people", `[{"id":4,"name":"Ada"},{"id":5,"name":2}]`)
	result, err = ExecuteSQLQuery(query, trie)
	if err == nil || len(result.Rows) != 0 {
		t.Fatalf("mixed-type UPPER query = %#v, error %v; want type error", result, err)
	}
}
