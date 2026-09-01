package hatCache

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONLowerIndexMaintainsCaseInsensitiveEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"name":"Ada"},
  {"id":2,"name":"ADA"},
  {"id":3,"name":"Grace"},
  {"id":4,"name":null},
  {"id":5}
]`)
	if err := trie.CreateSQLJSONLowerIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONLowerIndex() error = %v", err)
	}
	field := hatSql.LowerIndexField("name")
	rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", field, "ada")
	if err != nil || !available || len(rows) != 2 {
		t.Fatalf("initial lower index = %#v, available %t, error %v", rows, available, err)
	}
	query := "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'ada' SELECT person.id ORDER BY person.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["id"] != float64(2) {
		t.Fatalf("indexed LOWER query = %#v, error %v", result, err)
	}

	trie.UpsertString("people", `[{"id":6,"name":"AdA"}]`)
	result, err = ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(6) {
		t.Fatalf("refreshed LOWER query = %#v, error %v", result, err)
	}

	data := `[{"id":7,"name":"Ada"}]`
	trie.UpsertString("people", data)
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: len(data) - 1}); err != nil {
		t.Fatalf("SetSQLJSONIndexAdmissionBudget() error = %v", err)
	}
	result, err = ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(7) {
		t.Fatalf("scan fallback LOWER query = %#v, error %v", result, err)
	}
}

func TestSQLJSONLowerIndexFallsBackForUnconfiguredAndMixedTypeSources(t *testing.T) {
	t.Parallel()
	query := "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'ada' SELECT person.id"

	unconfigured := newTestTrie(t)
	unconfigured.UpsertString("people", `[{"id":1,"name":"Ada"}]`)
	result, err := ExecuteSQLQuery(query, unconfigured)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("unconfigured LOWER query = %#v, error %v", result, err)
	}

	mixed := newTestTrie(t)
	mixed.UpsertString("people", `[{"id":1,"name":"Ada"},{"id":2,"name":2}]`)
	if err := mixed.CreateSQLJSONLowerIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONLowerIndex() error = %v", err)
	}
	_, available, err := mixed.ResolveSQLIndexedSource("CACHE", "people", hatSql.LowerIndexField("name"), "ada")
	if err != nil || available {
		t.Fatalf("mixed lower index availability/error = %t/%v; want scan fallback", available, err)
	}
	_, err = ExecuteSQLQuery(query, mixed)
	if err == nil || !strings.Contains(err.Error(), "LOWER expects a TEXT argument") {
		t.Fatalf("mixed LOWER query error = %v, want type error", err)
	}
}

func TestSQLJSONLowerIndexFallsBackForNonStringLiteral(t *testing.T) {
	t.Parallel()
	query := "FROM CACHE('people') AS person WHERE LOWER(person.name) = 123 SELECT person.id"
	scan := newTestTrie(t)
	scan.UpsertString("people", `[{"id":1,"name":"123"},{"id":2,"name":"other"}]`)
	want, err := ExecuteSQLQuery(query, scan)
	if err != nil {
		t.Fatalf("scan LOWER query error = %v", err)
	}

	indexed := newTestTrie(t)
	indexed.UpsertString("people", `[{"id":1,"name":"123"},{"id":2,"name":"other"}]`)
	if err := indexed.CreateSQLJSONLowerIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONLowerIndex() error = %v", err)
	}
	_, available, err := indexed.ResolveSQLIndexedSource("CACHE", "people", hatSql.LowerIndexField("name"), float64(123))
	if err != nil || available {
		t.Fatalf("non-string lower index availability/error = %t/%v; want scan fallback", available, err)
	}
	got, err := ExecuteSQLQuery(query, indexed)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed LOWER query = %#v, %v; want %#v", got, err, want)
	}
}
