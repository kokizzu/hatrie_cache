package hatCache

import (
	"context"
	"testing"
)

func TestSQLJSONIndexAdmissionBudgetFallsBackToScan(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	data := `[{"id":1,"name":"Ada"},{"id":2,"name":"Grace"}]`
	trie.UpsertString("people", data)
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: len(data) - 1}); err != nil {
		t.Fatalf("SetSQLJSONIndexAdmissionBudget() error = %v", err)
	}
	if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	if err := trie.CreateSQLJSONBitmapIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "people", Fields: []string{"id"}, Type: SQLIndexInt64}); err != nil {
		t.Fatalf("CreateSQLTypedJSONIndex() error = %v", err)
	}
	if err := trie.CreateSQLJSONTextIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONTextIndex() error = %v", err)
	}
	if err := trie.CreateSQLJSONCoveringIndex("people", "id", "name"); err != nil {
		t.Fatalf("CreateSQLJSONCoveringIndex() error = %v", err)
	}
	if err := trie.CreateSQLJSONCompositeIndex("people", "id", "name"); err != nil {
		t.Fatalf("CreateSQLJSONCompositeIndex() error = %v", err)
	}
	if rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(2)); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLIndexedSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if rows, available, err := trie.ResolveSQLTextSource("CACHE", "people", "name", "ada"); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLTextSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if rows, available, err := trie.ResolveSQLCoveringSource("CACHE", "people", "id", float64(2), []string{"id", "name"}); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLCoveringSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if rows, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "people", []string{"id", "name"}, []interface{}{float64(2), "Grace"}); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLCompositeIndexedSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if rows, available, err := trie.ResolveSQLIndexedRangeSource("CACHE", "people", "id", ">=", float64(1)); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLIndexedRangeSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if rows, available, err := trie.ResolveSQLOrderedSource("CACHE", "people", "id", false, false, false); err != nil || available || rows != nil {
		t.Fatalf("denied ResolveSQLOrderedSource() = %#v, %v, %v; want scan fallback", rows, available, err)
	}
	if available, err := trie.StreamSQLOrderedSource(context.Background(), "CACHE", "people", "id", false, false, false, func(SQLRow) error {
		t.Fatal("denied StreamSQLOrderedSource() visited a row")
		return nil
	}); err != nil || available {
		t.Fatalf("denied StreamSQLOrderedSource() = %v, %v; want scan fallback", available, err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.id = 2 SELECT person.name", trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Grace" {
		t.Fatalf("fallback ExecuteSQLQuery() = %#v, %v", result, err)
	}
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: len(data)}); err != nil {
		t.Fatalf("raised SetSQLJSONIndexAdmissionBudget() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "id", float64(2))
	if err != nil || !available || len(rows) != 1 || rows[0]["name"] != "Grace" {
		t.Fatalf("admitted ResolveSQLIndexedSource() = %#v, %v, %v", rows, available, err)
	}
}
