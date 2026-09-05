package hatCache

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLJSONMultikeyIndexUsesArrayMembershipAndRefreshes(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"tags":["go","sql","go"]},
  {"id":2,"tags":["rust"]},
  {"id":3,"tags":[]},
  {"id":4,"tags":"go"},
  {"id":5}
]`)
	if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}

	rows, available, err := trie.ResolveSQLMultikeySource("CACHE", "people", "tags", "go")
	if err != nil || !available || !reflect.DeepEqual(rows, []SQLRow{{"id": float64(1), "tags": []interface{}{"go", "sql", "go"}}}) {
		t.Fatalf("ResolveSQLMultikeySource() = %#v, %v, %v", rows, available, err)
	}
	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "tags", "go"); err != nil || available {
		t.Fatalf("scalar ResolveSQLIndexedSource() = available %v, error %v; want unavailable", available, err)
	}

	result, err := ExecuteSQLQuery(`
FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie)
	if err != nil {
		t.Fatalf("ARRAY_CONTAINS query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}}) {
		t.Fatalf("ARRAY_CONTAINS query rows = %#v", result.Rows)
	}
	explained, err := ExecuteSQLQuery(`
EXPLAIN ANALYZE FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie)
	if err != nil {
		t.Fatalf("ARRAY_CONTAINS EXPLAIN error = %v", err)
	}
	if len(explained.Rows) == 0 || explained.Rows[0]["node"] != "MULTIKEY INDEX SCAN" {
		t.Fatalf("ARRAY_CONTAINS plan = %#v, want MULTIKEY INDEX SCAN", explained.Rows)
	}

	trie.UpsertString("people", `[
  {"id":1,"tags":["rust"]},
  {"id":2,"tags":["go"]}
]`)
	result, err = ExecuteSQLQuery(`
FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie)
	if err != nil {
		t.Fatalf("refreshed ARRAY_CONTAINS query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}}) {
		t.Fatalf("refreshed ARRAY_CONTAINS query rows = %#v", result.Rows)
	}
}

func TestSQLJSONMultikeyIndexReportsHealthAndConsistency(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"tags":["go","sql","go"]},
  {"id":2,"tags":["rust"]},
  {"id":3,"tags":[]},
  {"id":4,"tags":"go"},
  {"id":5}
]`)
	if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}

	health, available, err := trie.SQLJSONIndexHealth("people", "tags")
	if err != nil || !available {
		t.Fatalf("SQLJSONIndexHealth() = %#v, %v, %v", health, available, err)
	}
	if health.Rows != 5 || health.IndexedRows != 2 || health.NullRows != 3 || health.DistinctKeys != 3 || !health.Current {
		t.Fatalf("multikey health = %#v", health)
	}
	maintenance, available, err := trie.SQLJSONIndexMaintenanceStats("people", "tags")
	if err != nil || !available || maintenance.Configured != 1 || !maintenance.Current {
		t.Fatalf("multikey maintenance = %#v, %v, %v", maintenance, available, err)
	}
	consistency, available, err := trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available || !consistency.Consistent || len(consistency.Indexes) != 1 || consistency.Indexes[0].Kind != "multikey" {
		t.Fatalf("multikey consistency = %#v, %v, %v", consistency, available, err)
	}

	trie.sqlIndexMu.Lock()
	delete(trie.sqlJSONIndexes["people"]["tags"].rows, "s:go")
	trie.sqlIndexMu.Unlock()
	consistency, available, err = trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available || consistency.Consistent || consistency.Indexes[0].Consistent {
		t.Fatalf("corrupt multikey consistency = %#v, %v, %v", consistency, available, err)
	}
}

func TestSQLJSONMultikeyIndexDoesNotClaimScalarOrOrderedPlans(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"tags":["go"]}]`)
	if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}
	if _, _, available, err := trie.SQLJSONRangeEstimate("people", "tags", ">", "a"); err != nil || available {
		t.Fatalf("SQLJSONRangeEstimate() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.SQLJSONRangeStats("people", "tags", 4); err != nil || available {
		t.Fatalf("SQLJSONRangeStats() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.ResolveSQLIndexedRangeSource("CACHE", "people", "tags", ">", "a"); err != nil || available {
		t.Fatalf("ResolveSQLIndexedRangeSource() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.ResolveSQLPrefixSource("CACHE", "people", "tags", "g"); err != nil || available {
		t.Fatalf("ResolveSQLPrefixSource() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.ResolveSQLOrderedSource("CACHE", "people", "tags", false, false, false); err != nil || available {
		t.Fatalf("ResolveSQLOrderedSource() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.BorrowSQLIndexedSource("CACHE", "people", "tags", "go"); err != nil || available {
		t.Fatalf("BorrowSQLIndexedSource() = available %v, error %v; want unavailable", available, err)
	}
	if _, available, err := trie.BorrowSQLPrefixSource("CACHE", "people", "tags", "g"); err != nil || available {
		t.Fatalf("BorrowSQLPrefixSource() = available %v, error %v; want unavailable", available, err)
	}
	if available, err := trie.StreamSQLOrderedSource(context.Background(), "CACHE", "people", "tags", false, false, false, func(SQLRow) error { return nil }); err != nil || available {
		t.Fatalf("StreamSQLOrderedSource() = available %v, error %v; want unavailable", available, err)
	}
	if available, err := trie.StreamSQLOrderedSourceAfter(context.Background(), "CACHE", "people", "tags", false, false, false, SQLKeysetPosition{}, func(SQLRow, SQLKeysetPosition) error { return nil }); err != nil || available {
		t.Fatalf("StreamSQLOrderedSourceAfter() = available %v, error %v; want unavailable", available, err)
	}
	if _, exact, available, err := trie.SQLJSONIndexValueEstimate("people", "tags", "go"); err != nil || exact || available {
		t.Fatalf("SQLJSONIndexValueEstimate() = exact %v, available %v, error %v; want unavailable", exact, available, err)
	}
	if _, available, err := trie.SQLJSONIndexStats("people", "tags"); err != nil || available {
		t.Fatalf("SQLJSONIndexStats() = available %v, error %v; want unavailable", available, err)
	}
}

func TestSQLJSONArrayContainsFallsBackToScanWithoutIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"tags":["go"]},
  {"id":2,"tags":["rust"]}
]`)
	result, err := ExecuteSQLQuery(`
FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie)
	if err != nil {
		t.Fatalf("ARRAY_CONTAINS scan query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(1)}}) {
		t.Fatalf("ARRAY_CONTAINS scan query rows = %#v", result.Rows)
	}
}

func TestSQLJSONArrayContainsPreservesJSONValueEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("values", `[
  {"id":1,"items":[1,true,null]},
  {"id":2,"items":[2.5]},
  {"id":3,"items":["1"]}
]`)
	queries := []struct {
		where string
		want  []SQLRow
	}{
		{where: "ARRAY_CONTAINS(item.items, 1)", want: []SQLRow{{"id": float64(1)}, {"id": float64(3)}}},
		{where: "ARRAY_CONTAINS(item.items, TRUE)", want: []SQLRow{{"id": float64(1)}}},
		{where: "ARRAY_CONTAINS(item.items, 2.5)", want: []SQLRow{{"id": float64(2)}}},
		{where: "ARRAY_CONTAINS(item.items, NULL)", want: []SQLRow{}},
	}
	for _, query := range queries {
		result, err := ExecuteSQLQuery("FROM CACHE('values') AS item WHERE "+query.where+" SELECT item.id", trie)
		if err != nil {
			t.Fatalf("%s error = %v", query.where, err)
		}
		if !reflect.DeepEqual(result.Rows, query.want) {
			t.Fatalf("%s rows = %#v, want %#v", query.where, result.Rows, query.want)
		}
	}
}

func TestSQLJSONMultikeyIndexSupportsNumericAndBooleanElements(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("values", `[
  {"id":1,"items":[1,true]},
  {"id":2,"items":[2.5,false]},
  {"id":3,"items":["1"]}
]`)
	if err := trie.CreateSQLJSONMultikeyIndex("values", "items"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}
	for _, query := range []struct {
		value string
		want  []SQLRow
	}{
		{value: "1", want: []SQLRow{{"id": float64(1)}, {"id": float64(3)}}},
		{value: "TRUE", want: []SQLRow{{"id": float64(1)}}},
		{value: "2.5", want: []SQLRow{{"id": float64(2)}}},
		{value: "FALSE", want: []SQLRow{{"id": float64(2)}}},
	} {
		result, err := ExecuteSQLQuery("FROM CACHE('values') AS item WHERE ARRAY_CONTAINS(item.items, "+query.value+") SELECT item.id", trie)
		if err != nil {
			t.Fatalf("ARRAY_CONTAINS(%s) error = %v", query.value, err)
		}
		if !reflect.DeepEqual(result.Rows, query.want) {
			t.Fatalf("ARRAY_CONTAINS(%s) rows = %#v, want %#v", query.value, result.Rows, query.want)
		}
	}
}

func TestSQLJSONMultikeyIndexMatchesSQLMixedNumericEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("values", `[
  {"id":1,"items":[1]},
  {"id":2,"items":["1"]},
  {"id":3,"items":["01"]}
]`)
	if err := trie.CreateSQLJSONMultikeyIndex("values", "items"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('values') AS item WHERE ARRAY_CONTAINS(item.items, 1) SELECT item.id", trie)
	if err != nil {
		t.Fatalf("ARRAY_CONTAINS numeric query error = %v", err)
	}
	want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ARRAY_CONTAINS numeric query rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLJSONMultikeyIndexFallsBackForUnicodeCollation(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"tags":["Go"]}]`)
	if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
		t.Fatalf("CreateSQLJSONMultikeyIndex() error = %v", err)
	}
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie, SQLQueryOptions{Collation: SQLCollationUnicodeCI})
	if err != nil {
		t.Fatalf("Unicode ARRAY_CONTAINS query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("Unicode ARRAY_CONTAINS query rows = %#v, want %#v", result.Rows, want)
	}
}
