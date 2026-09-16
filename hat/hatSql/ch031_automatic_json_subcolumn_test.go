package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestCH031AutomaticJSONSubcolumnPromotesAfterThresholdAndTracksGeneration(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 3,
		MaxEntries:      4,
		MaxRows:         16,
		MaxBytes:        1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := hatSql.JSONSubcolumnAutoKey{
		SourceName: "CACHE",
		SourceKey:  "people",
		Field:      "doc",
		Path:       " $.user.id ",
		Generation: 7,
	}
	documents := []interface{}{
		`{"user":{"id":1}}`,
		`{"user":{"id":2}}`,
		`{"user":{"name":"missing"}}`,
	}
	for observation := 0; observation < 2; observation++ {
		column, ready, err := materializer.Observe(key, documents)
		if err != nil {
			t.Fatalf("observation %d error = %v", observation, err)
		}
		if ready || column.Rows != 0 {
			t.Fatalf("observation %d = rows %d, ready %v, want not ready", observation, column.Rows, ready)
		}
	}
	column, ready, err := materializer.Observe(key, documents)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || column.Kind != hatSql.ColumnarJSONSubcolumnFloat64 || column.Rows != len(documents) {
		t.Fatalf("promoted column = %#v, ready %v", column, ready)
	}
	if value, present := column.Value(1); !present || value != float64(2) {
		t.Fatalf("promoted Value(1) = %#v, %v", value, present)
	}
	if value, present := column.Value(2); present || value != nil {
		t.Fatalf("promoted missing Value(2) = %#v, %v", value, present)
	}

	lookup, found, err := materializer.Lookup(key)
	if err != nil {
		t.Fatal(err)
	}
	if !found || lookup.Rows != column.Rows || lookup.Kind != column.Kind {
		t.Fatalf("Lookup() = %#v, %v; want promoted column", lookup, found)
	}

	nextGeneration := key
	nextGeneration.Generation++
	if _, found, err := materializer.Lookup(nextGeneration); err != nil || found {
		t.Fatalf("new generation Lookup() found = %v, err = %v; want miss", found, err)
	}
	for observation := 0; observation < 3; observation++ {
		_, ready, err := materializer.Observe(nextGeneration, []interface{}{`{"user":{"id":9}}`})
		if err != nil {
			t.Fatal(err)
		}
		if ready != (observation == 2) {
			t.Fatalf("new generation observation %d ready = %v", observation, ready)
		}
	}
	stats := materializer.Stats()
	if stats.Promotions != 2 || stats.Entries != 1 || stats.Hits != 1 {
		t.Fatalf("stats = %#v, want two promotions, one entry, one hit", stats)
	}
}

func TestCH031AutomaticJSONSubcolumnResolveBatchPreservesRowsAndNullSemantics(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 2,
		MaxRows:         16,
	})
	if err != nil {
		t.Fatal(err)
	}
	source := hatSql.JSONSubcolumnAutoSource{SourceName: "CACHE", SourceKey: "people", Generation: 11}
	rows := []hatSql.Row{
		{"doc": `{"user":{"id":1}}`, "name": "Ada"},
		{"doc": `{"user":null}`, "name": "Grace"},
		{"doc": `{"user":{"id":null}}`, "name": "Lin"},
	}
	paths := []hatSql.ColumnarJSONSubcolumnRequest{{Field: "doc", Path: "$.user.id"}}
	if batch, available, err := materializer.ResolveBatch(source, []string{"name"}, paths, rows); err != nil || available || batch.Rows != 0 {
		t.Fatalf("cold ResolveBatch() = rows %d, available %v, err %v; want unavailable", batch.Rows, available, err)
	}
	batch, available, err := materializer.ResolveBatch(source, []string{"name"}, paths, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !available || batch.Rows != len(rows) || batch.FieldRows("name") != len(rows) {
		t.Fatalf("warm ResolveBatch() = rows %d, name rows %d, available %v", batch.Rows, batch.FieldRows("name"), available)
	}
	column, found := batch.JSONSubcolumns[hatSql.ColumnarJSONSubcolumnKey{Field: "doc", Path: "$.user.id"}]
	if !found {
		t.Fatal("warm batch did not contain the requested JSON subcolumn")
	}
	for row, want := range []struct {
		value   interface{}
		present bool
	}{{float64(1), true}, {nil, false}, {nil, true}} {
		value, present := column.Value(row)
		if present != want.present || !reflect.DeepEqual(value, want.value) {
			t.Fatalf("row %d Value() = %#v, %v; want %#v, %v", row, value, present, want.value, want.present)
		}
	}
}

func TestCH031AutomaticJSONSubcolumnRejectsUnsupportedAndBoundsMemory(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxEntries:      1,
		MaxRows:         2,
		MaxBytes:        64,
	})
	if err != nil {
		t.Fatal(err)
	}
	oversized := hatSql.JSONSubcolumnAutoKey{Field: "doc", Path: "$.id", Generation: 1}
	if _, ready, err := materializer.Observe(oversized, []interface{}{`{"id":1}`, `{"id":2}`, `{"id":3}`}); err != nil || ready {
		t.Fatalf("oversized Observe() = ready %v, err %v; want unavailable", ready, err)
	}
	mixed := oversized
	mixed.Path = "$.value"
	if _, ready, err := materializer.Observe(mixed, []interface{}{`{"value":1}`, `{"value":"wrong"}`}); err != nil || ready {
		t.Fatalf("mixed Observe() = ready %v, err %v; want unavailable", ready, err)
	}
	if _, ready, err := materializer.Observe(mixed, []interface{}{`{"value":1}`, `{"value":"wrong"}`}); err != nil || ready {
		t.Fatalf("repeated mixed Observe() = ready %v, err %v; want unavailable", ready, err)
	}
	stats := materializer.Stats()
	if stats.Promotions != 0 || stats.Rejections != 1 || stats.Entries != 1 || stats.RetainedBytes != 0 {
		t.Fatalf("rejection stats = %#v", stats)
	}
}

func TestCH031AutomaticJSONSubcolumnEvictsAndInvalidatesBoundedEntries(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxEntries:      1,
		MaxRows:         4,
		MaxBytes:        1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := hatSql.JSONSubcolumnAutoKey{SourceName: "CACHE", SourceKey: "items", Field: "doc", Path: "$.id", Generation: 1}
	second := first
	second.Path = "$.name"
	if _, ready, err := materializer.Observe(first, []interface{}{`{"id":1}`}); err != nil || !ready {
		t.Fatalf("first Observe() = ready %v, err %v", ready, err)
	}
	if _, ready, err := materializer.Observe(second, []interface{}{`{"name":"Ada"}`}); err != nil || !ready {
		t.Fatalf("second Observe() = ready %v, err %v", ready, err)
	}
	if _, found, err := materializer.Lookup(first); err != nil || found {
		t.Fatalf("evicted first Lookup() found = %v, err = %v", found, err)
	}
	if _, found, err := materializer.Lookup(second); err != nil || !found {
		t.Fatalf("retained second Lookup() found = %v, err = %v", found, err)
	}
	materializer.InvalidateSource("CACHE", "items")
	if _, found, err := materializer.Lookup(second); err != nil || found {
		t.Fatalf("invalidated Lookup() found = %v, err = %v", found, err)
	}
	stats := materializer.Stats()
	if stats.Evictions != 1 || stats.Entries != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("eviction/invalidation stats = %#v", stats)
	}
}

func TestCH031AutomaticJSONSubcolumnRejectsInvalidConfigurationAndRetainedBytes(t *testing.T) {
	if _, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MaxEntries: -1}); err == nil {
		t.Fatal("negative MaxEntries was accepted")
	}
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxBytes:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := hatSql.JSONSubcolumnAutoKey{Field: "doc", Path: "$.name", Generation: 1}
	if _, ready, err := materializer.Observe(key, []interface{}{`{"name":"Ada"}`}); err != nil || ready {
		t.Fatalf("retained-byte-limited Observe() = ready %v, err %v", ready, err)
	}
	if stats := materializer.Stats(); stats.Rejections != 1 || stats.RetainedBytes != 0 {
		t.Fatalf("retained-byte rejection stats = %#v", stats)
	}
}

func TestCH031AutomaticJSONSubcolumnWorksThroughSQLResolverContract(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MinObservations: 2})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &ch031AutomaticJSONResolver{
		materializer: materializer,
		rows:         []hatSql.Row{{"doc": `{"id":7}`}, {"doc": `{"id":8}`}},
	}
	query := "SELECT JSON_VALUE(doc, '$.id') AS id FROM CACHE('items')"
	for run := 0; run < 2; run++ {
		result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			t.Fatalf("run %d error = %v", run, err)
		}
		want := []hatSql.SQLRow{{"id": float64(7)}, {"id": float64(8)}}
		if !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("run %d rows = %#v, want %#v", run, result.Rows, want)
		}
	}
	if resolver.columnarCalls != 2 || resolver.sourceCalls != 1 {
		t.Fatalf("resolver calls = columnar %d, source %d; want two columnar calls and one fallback read during promotion", resolver.columnarCalls, resolver.sourceCalls)
	}
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.columnarCalls != 3 || resolver.sourceCalls != 1 {
		t.Fatalf("warm resolver calls = columnar %d, source %d; want columnar only", resolver.columnarCalls, resolver.sourceCalls)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.SQLRow{{"id": float64(7)}, {"id": float64(8)}}) {
		t.Fatalf("warm rows = %#v", result.Rows)
	}
}

type ch031AutomaticJSONResolver struct {
	materializer  *hatSql.JSONSubcolumnAutoMaterializer
	rows          []hatSql.Row
	sourceCalls   int
	columnarCalls int
}

func (resolver *ch031AutomaticJSONResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	resolver.sourceCalls++
	return resolver.rows, nil
}

func (resolver *ch031AutomaticJSONResolver) ResolveSQLColumnarSource(string, string, []string) (hatSql.ColumnarBatch, bool, error) {
	return hatSql.ColumnarBatch{}, false, nil
}

func (resolver *ch031AutomaticJSONResolver) ResolveSQLColumnarJSONSubcolumns(name, key string, fields []string, paths []hatSql.ColumnarJSONSubcolumnRequest) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	resolver.columnarCalls++
	batch, available, err := resolver.materializer.ResolveBatch(
		hatSql.JSONSubcolumnAutoSource{SourceName: name, SourceKey: key, Generation: 1},
		fields,
		paths,
		resolver.rows,
	)
	return batch, nil, available, err
}
