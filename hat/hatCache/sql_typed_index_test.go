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
	if _, generic := trie.sqlJSONIndexes["people"]["age"]; generic {
		t.Fatal("typed index unexpectedly configured a generic field index")
	}
}
