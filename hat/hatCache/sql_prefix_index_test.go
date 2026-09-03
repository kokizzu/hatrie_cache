package hatCache

import (
	"reflect"
	"strings"
	"testing"
)

func TestSQLJSONFieldIndexAcceleratesLikePrefix(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"name":"alice"},
  {"id":2,"name":"alex"},
  {"id":3,"name":"bob"},
  {"id":4,"name":"al"},
  {"id":5,"name":null},
  {"id":6}
]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}

	query := "FROM CACHE('people') AS person WHERE person.name LIKE 'al%' SELECT person.id ORDER BY person.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("prefix query error = %v", err)
	}
	want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("prefix query rows = %#v, want %#v", result.Rows, want)
	}

	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("prefix EXPLAIN error = %v", err)
	}
	if !sqlPrefixPlanHasNode(explained.Rows, "INDEX PREFIX SCAN") {
		t.Fatalf("prefix plan = %#v, want INDEX PREFIX SCAN", explained.Rows)
	}

	allStringsQuery := "FROM CACHE('people') AS person WHERE person.name LIKE '%' SELECT person.id ORDER BY person.id"
	allStrings, err := ExecuteSQLQuery(allStringsQuery, trie)
	if err != nil {
		t.Fatalf("all-strings prefix query error = %v", err)
	}
	allStringsWant := []SQLRow{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(3)}, {"id": float64(4)}}
	if !reflect.DeepEqual(allStrings.Rows, allStringsWant) {
		t.Fatalf("all-strings prefix rows = %#v, want %#v", allStrings.Rows, allStringsWant)
	}
	allStringsExplain, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+allStringsQuery, trie)
	if err != nil {
		t.Fatalf("all-strings prefix EXPLAIN error = %v", err)
	}
	if !sqlPrefixPlanHasNode(allStringsExplain.Rows, "INDEX PREFIX SCAN") {
		t.Fatalf("all-strings prefix plan = %#v, want INDEX PREFIX SCAN", allStringsExplain.Rows)
	}
}

func TestSQLJSONFieldIndexLikePrefixFallsBackForUnsafePatternsAndTypes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("mixed", `[
  {"id":1,"name":"alpha"},
  {"id":2,"name":123},
  {"id":3,"name":"beta"}
]`)
	if err := trie.CreateSQLJSONFieldIndex("mixed", "name"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}

	query := "FROM CACHE('mixed') AS item WHERE item.name LIKE '12%' SELECT item.id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("mixed-type prefix query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("mixed-type prefix rows = %#v", result.Rows)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("mixed-type prefix EXPLAIN error = %v", err)
	}
	if sqlPrefixPlanHasNode(explained.Rows, "INDEX PREFIX SCAN") {
		t.Fatalf("mixed-type prefix unexpectedly used index: %#v", explained.Rows)
	}

	unsafeQuery := "FROM CACHE('mixed') AS item WHERE item.name LIKE '%a%' SELECT item.id"
	unsafeResult, err := ExecuteSQLQuery(unsafeQuery, trie)
	if err != nil {
		t.Fatalf("contains-like query error = %v", err)
	}
	if !reflect.DeepEqual(unsafeResult.Rows, []SQLRow{{"id": float64(1)}, {"id": float64(3)}}) {
		t.Fatalf("contains-like rows = %#v", unsafeResult.Rows)
	}
	unsafeExplain, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+unsafeQuery, trie)
	if err != nil {
		t.Fatalf("contains-like EXPLAIN error = %v", err)
	}
	if sqlPrefixPlanHasNode(unsafeExplain.Rows, "INDEX PREFIX SCAN") {
		t.Fatalf("contains-like query unexpectedly used prefix index: %#v", unsafeExplain.Rows)
	}
}

func TestSQLJSONFieldIndexLikePrefixRefreshesAfterReplacement(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"alpha"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	query := "FROM CACHE('people') AS person WHERE person.name LIKE 'al%' SELECT person.id"
	if _, err := ExecuteSQLQuery(query, trie); err != nil {
		t.Fatalf("initial prefix query error = %v", err)
	}

	trie.UpsertString("people", `[{"id":2,"name":"beta"}]`)
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.name LIKE 'be%' SELECT person.id", trie)
	if err != nil {
		t.Fatalf("refreshed prefix query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("refreshed prefix rows = %#v", result.Rows)
	}
}

func sqlPrefixPlanHasNode(rows []SQLRow, node string) bool {
	for _, row := range rows {
		if rowNode, ok := row["node"].(string); ok && strings.EqualFold(rowNode, node) {
			return true
		}
	}
	return false
}
