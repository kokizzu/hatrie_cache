package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestTypedTableColumnarSegmentsBuildDictionaryCodeSets(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:        true,
			RowsPerSegment: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"red", "red", "blue", "blue", "red"} {
		if _, err := table.Upsert("event-"+string(rune('a'+index)), []TypedTableValue{TypedString(team), TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}

	table.mu.RLock()
	batch := table.columnarBatchLocked([]string{"team", "score"})
	segments := table.columnarNumericSegmentsLocked(batch)
	table.mu.RUnlock()
	if segments == nil {
		t.Fatal("columnar segments are nil")
	}
	sets, ok := segments.DictionaryCodeSets["team"]
	if !ok {
		t.Fatal("dictionary code sets are missing")
	}
	want := []uint64{1, 2, 1}
	if len(sets) != len(want) {
		t.Fatalf("dictionary code set count = %d, want %d", len(sets), len(want))
	}
	for index, got := range sets {
		if got != want[index] {
			t.Fatalf("dictionary code set %d = %d, want %d", index, got, want[index])
		}
	}
}

func TestTypedTableColumnarSegmentsExposeDictionaryCodeSets(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:        true,
			RowsPerSegment: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"red", "red", "blue", "blue", "red"} {
		if _, err := table.Upsert("event-"+string(rune('a'+index)), []TypedTableValue{TypedString(team)}); err != nil {
			t.Fatal(err)
		}
	}
	fields := []string{"team"}
	for read := 0; read < 2; read++ {
		if _, found, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !found {
			t.Fatalf("ResolveSQLColumnarSource() = found %t, error %v; want found", found, err)
		}
	}
	batch, segments, found, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !found {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = found %t, error %v; want found", found, err)
	}
	if segments == nil {
		t.Fatal("borrowed columnar segments are nil")
	}
	sets, ok := segments.DictionaryCodeSets["team"]
	if !ok || len(sets) != 3 {
		t.Fatalf("borrowed dictionary code sets = %#v, want three segments", sets)
	}
	if got, want := typedTableColumnarBatchBytes(batch, segments)-typedTableColumnarBatchBytes(batch, nil), 64+len(sets)*8; got != want {
		t.Fatalf("dictionary sidecar byte charge = %d, want %d", got, want)
	}
}

func TestTypedTableColumnarSegmentsSkipUntrustedAndWideDictionaries(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:        true,
			RowsPerSegment: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, dictionary := range map[string]DictionaryColumn{
		"untrusted": {Values: []string{"red", "blue"}, Codes: []uint32{0, 1, 0, 1}},
		"wide": {
			Values: func() []string {
				values := make([]string, 65)
				for index := range values {
					values[index] = "value-" + string(rune('a'+index%26))
				}
				return values
			}(),
			Codes: func() []uint32 {
				codes := make([]uint32, 65)
				for index := range codes {
					codes[index] = uint32(index)
				}
				return codes
			}(),
			codesTrusted: true,
		},
	} {
		batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"team": dictionary}, Rows: dictionary.RowCount()}
		segments := table.columnarNumericSegmentsLocked(batch)
		if segments != nil && len(segments.DictionaryCodeSets) != 0 {
			t.Fatalf("%s dictionary unexpectedly created code sets: %#v", name, segments.DictionaryCodeSets)
		}
	}
}

func TestTypedTableColumnarSegmentsRejectMalformedTrustedDictionaryCodes(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:        true,
			RowsPerSegment: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"red"}, Codes: []uint32{1}, codesTrusted: true},
		},
		Rows: 1,
	}
	segments := table.columnarNumericSegmentsLocked(batch)
	if segments != nil && len(segments.DictionaryCodeSets) != 0 {
		t.Fatalf("malformed trusted dictionary received code sets: %#v", segments.DictionaryCodeSets)
	}
}

func TestTypedTableColumnarDictionaryCodeSetsPreserveSQLResults(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:           true,
			CompressedBatches: true,
			RowsPerSegment:    2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"red", "red", "blue", "blue", "red"} {
		if _, err := table.Upsert("event-"+string(rune('a'+index)), []TypedTableValue{TypedString(team)}); err != nil {
			t.Fatal(err)
		}
	}

	queries := []struct {
		name string
		sql  string
		want []SQLRow
	}{
		{
			name: "equality",
			sql:  "SELECT COUNT(*) AS total FROM CACHE('events') WHERE team = 'red'",
			want: []SQLRow{{"total": int64(3)}},
		},
		{
			name: "in",
			sql:  "SELECT COUNT(*) AS total FROM CACHE('events') WHERE team IN ('red')",
			want: []SQLRow{{"total": int64(3)}},
		},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			for read := 0; read < 2; read++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query.sql, table, SQLQueryOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(result.Rows, query.want) {
					t.Fatalf("rows = %#v, want %#v", result.Rows, query.want)
				}
			}
		})
	}

	batch, segments, found, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", []string{"team"})
	if err != nil {
		t.Fatal(err)
	}
	if !found || batch.Rows != 5 || segments == nil || len(segments.DictionaryCodeSets["team"]) != 3 {
		t.Fatalf("cached dictionary segments = found:%v rows:%d segments:%#v, want 5 rows and 3 masks", found, batch.Rows, segments)
	}
}
