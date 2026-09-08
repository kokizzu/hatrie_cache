package hatSql

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func TestDictionaryColumnPackedCodesRoundTrip(t *testing.T) {
	legacy := DictionaryColumn{Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1, 0}}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"team": legacy}, Rows: len(legacy.Codes)}
	batch.PackDictionaryCodes()

	dictionary := batch.Dictionaries["team"]
	if dictionary.CodeWidth != 1 {
		t.Fatalf("packed code width = %d, want 1", dictionary.CodeWidth)
	}
	if dictionary.Codes != nil {
		t.Fatalf("legacy codes retained after packing: %#v", dictionary.Codes)
	}
	if len(dictionary.PackedCodes) != len(legacy.Codes) {
		t.Fatalf("packed code bytes = %d, want %d", len(dictionary.PackedCodes), len(legacy.Codes))
	}
	if got := batch.FieldRows("team"); got != len(legacy.Codes) {
		t.Fatalf("packed field rows = %d, want %d", got, len(legacy.Codes))
	}
	for row, want := range []interface{}{"ops", "core", "data", "core", "ops"} {
		if got, ok := batch.Value("team", row); !ok || got != want {
			t.Fatalf("packed value at row %d = %#v/%v, want %#v/true", row, got, ok, want)
		}
	}
	if _, ok := dictionary.CodeAt(len(legacy.Codes)); ok {
		t.Fatal("packed code lookup past the row count unexpectedly succeeded")
	}
}

func TestDictionaryColumnPackedWideCodesRoundTrip(t *testing.T) {
	values := make([]string, 257)
	for index := range values {
		values[index] = "value-" + strconv.Itoa(index)
	}
	codes := []uint32{0, 255, 256, 1, 256}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"value": {Values: values, Codes: codes},
	}, Rows: len(codes)}
	batch.PackDictionaryCodes()

	dictionary := batch.Dictionaries["value"]
	if dictionary.CodeWidth != 2 {
		t.Fatalf("packed wide code width = %d, want 2", dictionary.CodeWidth)
	}
	if len(dictionary.PackedCodes) != len(codes)*2 {
		t.Fatalf("packed wide code bytes = %d, want %d", len(dictionary.PackedCodes), len(codes)*2)
	}
	for row, want := range codes {
		got, ok := dictionary.CodeAt(row)
		if !ok || got != want {
			t.Fatalf("packed wide code at row %d = %d/%v, want %d/true", row, got, ok, want)
		}
	}
}

func TestPackDictionaryCodesLeavesInvalidLegacyCodesUntouched(t *testing.T) {
	legacy := []uint32{0, 2}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"team": {Values: []string{"ops", "core"}, Codes: legacy},
	}, Rows: len(legacy)}
	batch.PackDictionaryCodes()

	dictionary := batch.Dictionaries["team"]
	if !reflect.DeepEqual(dictionary.Codes, legacy) || len(dictionary.PackedCodes) != 0 || dictionary.CodeWidth != 0 {
		t.Fatalf("invalid dictionary was changed: %#v", dictionary)
	}
}

func TestTypedTableColumnarProducerKeepsLegacyDictionaryCodesByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"ops", "core", "ops", "core"} {
		if _, err := table.Upsert("event-"+strconv.Itoa(index), []TypedTableValue{TypedString(team)}); err != nil {
			t.Fatal(err)
		}
	}
	table.mu.RLock()
	batch := table.columnarBatchLocked([]string{"team"})
	table.mu.RUnlock()
	dictionary := batch.Dictionaries["team"]
	if dictionary.CodeWidth != 0 || len(dictionary.Codes) != batch.Rows || len(dictionary.PackedCodes) != 0 {
		t.Fatalf("typed-table dictionary layout = %#v, rows = %d", dictionary, batch.Rows)
	}
	batch.PackDictionaryCodes()
	dictionary = batch.Dictionaries["team"]
	if dictionary.CodeWidth != 1 || dictionary.Codes != nil || len(dictionary.PackedCodes) != batch.Rows {
		t.Fatalf("explicitly packed typed-table dictionary layout = %#v, rows = %d", dictionary, batch.Rows)
	}
}

type packedDictionaryQueryResolver struct {
	batch         ColumnarBatch
	columnarCalls int
}

func (resolver packedDictionaryQueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for packed dictionary queries")
}

func (resolver packedDictionaryQueryResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestPackedDictionaryQueriesPreserveResults(t *testing.T) {
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}
	batch.PackDictionaryCodes()
	tests := []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{
			name:  "distinct_where",
			query: "SELECT DISTINCT team FROM CACHE('items') WHERE team IN ('core', 'ops') ORDER BY team",
			want:  []SQLRow{{"team": "core"}, {"team": "ops"}},
		},
		{
			name:  "distinct",
			query: "SELECT DISTINCT team FROM CACHE('items') ORDER BY team",
			want:  []SQLRow{{"team": "core"}, {"team": "data"}, {"team": "ops"}},
		},
		{
			name:  "group",
			query: "SELECT team, COUNT(*) AS total, SUM(score) AS sum FROM CACHE('items') GROUP BY team ORDER BY team",
			want: []SQLRow{
				{"team": "core", "total": int64(2), "sum": float64(30)},
				{"team": "data", "total": int64(1), "sum": float64(12)},
				{"team": "ops", "total": int64(1), "sum": float64(7)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := packedDictionaryQueryResolver{batch: batch}
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func BenchmarkColumnarDictionaryPackedQueries(b *testing.B) {
	for _, packed := range []bool{false, true} {
		name := "legacy_uint32"
		if packed {
			name = "packed_uint8"
		}
		b.Run(name, func(b *testing.B) {
			batch := newPackedDictionaryBenchmarkBatch(packed)
			resolver := packedDictionaryQueryResolver{batch: batch}
			query := "SELECT DISTINCT team FROM CACHE('items') ORDER BY team"
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
				if err != nil || len(result.Rows) != 8 {
					b.Fatalf("ExecuteSQLQueryParameters() = %#v, %v", result.Rows, err)
				}
			}
			b.ReportMetric(float64(batch.Dictionaries["team"].RowCount()), "rows/op")
			b.ReportMetric(float64(dictionaryCodeBytes(batch.Dictionaries["team"])), "code-bytes/op")
		})
	}
}

func newPackedDictionaryBenchmarkBatch(packed bool) ColumnarBatch {
	const rows = 4096
	const values = 8
	codes := make([]uint32, rows)
	dictionaryValues := make([]string, values)
	for index := range dictionaryValues {
		dictionaryValues[index] = "team-" + strconv.Itoa(index)
	}
	for index := range codes {
		codes[index] = uint32(index % values)
	}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"team": {Values: dictionaryValues, Codes: codes},
	}, Rows: rows}
	if packed {
		batch.PackDictionaryCodes()
	}
	return batch
}

func dictionaryCodeBytes(dictionary DictionaryColumn) int {
	if dictionary.Codes != nil {
		return len(dictionary.Codes) * 4
	}
	return len(dictionary.PackedCodes)
}
