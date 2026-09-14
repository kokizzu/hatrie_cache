package hatSql

import (
	"reflect"
	"testing"
)

func TestTR019ColumnarBatchPrepareFieldOffsetsPreservesValues(t *testing.T) {
	batch := tr019MixedColumnarBatch()
	batch.Columns["team"] = []interface{}{"plain-shadow"}
	before := make(map[string]interface{}, 5)
	for index, field := range []string{"team", "score", "active", "ratio", "name"} {
		value, valid := batch.Value(field, index)
		if !valid {
			t.Fatalf("baseline Value(%q, %d) was invalid", field, index)
		}
		before[field] = value
	}

	batch.PrepareFieldOffsets()
	if len(batch.fieldOffsets.offsets) != 5 {
		t.Fatalf("field offset count = %d, want 5", len(batch.fieldOffsets.offsets))
	}
	for index, field := range []string{"team", "score", "active", "ratio", "name"} {
		value, valid := batch.Value(field, index)
		if !valid || !reflect.DeepEqual(value, before[field]) {
			t.Fatalf("cached Value(%q, %d) = %#v/%t, want %#v/true", field, index, value, valid, before[field])
		}
	}
	if value, valid := batch.Value("missing", 0); valid || value != nil {
		t.Fatalf("cached missing Value() = %#v/%t, want nil/false", value, valid)
	}
}

func TestTR019ColumnarBatchPrepareFieldOffsetsInvalidatesOnMutation(t *testing.T) {
	batch := tr019MixedColumnarBatch()
	batch.PrepareFieldOffsets()
	if batch.fieldOffsets == nil {
		t.Fatal("PrepareFieldOffsets() did not build a cache")
	}
	batch.PackCompressedColumns()
	if batch.fieldOffsets != nil {
		t.Fatal("PackCompressedColumns() retained stale field offsets")
	}
	batch.PrepareFieldOffsets()
	if batch.fieldOffsets == nil {
		t.Fatal("PrepareFieldOffsets() did not rebuild after mutation")
	}

	plain := ColumnarBatch{Columns: map[string][]interface{}{"name": {"value"}}}
	plain.PrepareFieldOffsets()
	if plain.fieldOffsets != nil {
		t.Fatal("single-map batch unexpectedly allocated field offsets")
	}
}

func TestTR019TypedTableFieldOffsetCacheIsOptIn(t *testing.T) {
	schema := TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString, DictionaryEncoded: true},
			{Name: "score", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{Enabled: true, FieldOffsetCache: true, MinReads: 1},
	}
	cached, err := NewTypedTable(schema)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		if _, err := cached.Upsert(string(rune('a'+index)), []TypedTableValue{TypedString("red"), TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	batch, found, err := cached.ResolveSQLColumnarSource("CACHE", "events", []string{"team", "score"})
	if err != nil || !found {
		t.Fatalf("cached ResolveSQLColumnarSource() = found %t, error %v", found, err)
	}
	if batch.fieldOffsets == nil {
		t.Fatal("enabled FieldOffsetCache did not prepare the batch")
	}

	legacy, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString, DictionaryEncoded: true},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Upsert("a", []TypedTableValue{TypedString("red"), TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	legacyBatch, found, err := legacy.ResolveSQLColumnarSource("CACHE", "events", []string{"team", "score"})
	if err != nil || !found {
		t.Fatalf("legacy ResolveSQLColumnarSource() = found %t, error %v", found, err)
	}
	if legacyBatch.fieldOffsets != nil {
		t.Fatal("default columnar cache unexpectedly prepared field offsets")
	}
}
