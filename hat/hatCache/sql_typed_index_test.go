package hatCache

import (
	"reflect"
	"testing"
)

func TestSQLTypedInt64IndexAcceleratesEqualityRangeAndOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"age":30},{"id":2,"age":18},{"id":3,"age":21},{"id":4,"age":21}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
		CacheKey: "people",
		Fields:   []string{"age"},
		Type:     SQLIndexInt64,
	}); err != nil {
		t.Fatalf("CreateSQLTypedJSONIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.age >= 21 SELECT person.id, person.age ORDER BY person.age, person.id", trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{{"id": float64(3), "age": float64(21)}, {"id": float64(4), "age": float64(21)}, {"id": float64(1), "age": float64(30)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	equality, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "age", int64(21))
	if err != nil || !available || len(equality) != 2 {
		t.Fatalf("ResolveSQLIndexedSource() = %#v, %v, %v", equality, available, err)
	}
	ranged, available, err := trie.ResolveSQLIndexedRangeSource("CACHE", "people", "age", ">=", int64(21))
	if err != nil || !available || len(ranged) != 3 {
		t.Fatalf("ResolveSQLIndexedRangeSource() = %#v, %v, %v", ranged, available, err)
	}
	ordered, available, err := trie.ResolveSQLOrderedSource("CACHE", "people", "age", false, false, false)
	if err != nil || !available || len(ordered) != 4 || ordered[0]["id"] != float64(2) || ordered[3]["id"] != float64(1) {
		t.Fatalf("ResolveSQLOrderedSource() = %#v, %v, %v", ordered, available, err)
	}
	if _, generic := trie.sqlJSONIndexes["people"]["age"]; generic {
		t.Fatal("typed index unexpectedly configured a generic field index")
	}
}

func TestSQLTypedInt64IndexDeclinesMixedValueOrder(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"age":21},{"id":2,"age":"unknown"}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "people", Fields: []string{"age"}, Type: SQLIndexInt64}); err != nil {
		t.Fatal(err)
	}
	if _, available, err := trie.ResolveSQLOrderedSource("CACHE", "people", "age", false, false, false); err != nil || available {
		t.Fatalf("ResolveSQLOrderedSource() = available %v, error %v, want unavailable fallback", available, err)
	}
}

func TestSQLQueryUsesTypedInt64Order(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"age":30},{"id":2,"age":18},{"id":3,"age":21}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "people", Fields: []string{"age"}, Type: SQLIndexInt64}); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person SELECT person.id ORDER BY person.age LIMIT 2", trie)
	if err != nil || !reflect.DeepEqual(result.Rows, []SQLRow{{"id": float64(2)}, {"id": float64(3)}}) {
		t.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
	}
	trie.sqlIndexMu.RLock()
	raw := trie.sqlJSONTypedInt64Indexes["people"]["age"].raw
	trie.sqlIndexMu.RUnlock()
	if raw == "" {
		t.Fatal("compatible ORDER BY query did not build the typed index")
	}
}

func TestSQLTypedInt64IndexParticipatesInScheduledMaintenance(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"age":21}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "people", Fields: []string{"age"}, Type: SQLIndexInt64}); err != nil {
		t.Fatal(err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("people", "age"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}
	processed, err := trie.RunScheduledSQLJSONIndexRebuilds(1)
	if err != nil || processed != 1 {
		t.Fatalf("RunScheduledSQLJSONIndexRebuilds() = %d, %v", processed, err)
	}
	status, available, err := trie.SQLJSONIndexMaintenanceStats("people", "age")
	if err != nil || !available || !status.Current || status.Rebuilds != 1 {
		t.Fatalf("SQLJSONIndexMaintenanceStats() = %#v, %v, %v", status, available, err)
	}
	stats, available, err := trie.SQLJSONIndexStats("people", "age")
	if err != nil || !available || stats.Rows != 1 || stats.DistinctKeys != 1 {
		t.Fatalf("SQLJSONIndexStats() = %#v, %v, %v", stats, available, err)
	}
}
