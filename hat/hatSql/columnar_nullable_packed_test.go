package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type packedNullableQueryResolver struct {
	batch ColumnarBatch
}

func (resolver packedNullableQueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a packed nullable column")
}

func (resolver packedNullableQueryResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestColumnarBatchPackNullableColumnsRoundTrip(t *testing.T) {
	values := []interface{}{int64(7), nil, "ready", nil, float64(3.5), nil, true}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": values},
		Rows:    len(values),
	}

	batch.PackNullableColumns()

	if _, ok := batch.Columns["value"]; ok {
		t.Fatal("packed column still has a legacy column")
	}
	packed, ok := batch.PackedColumns["value"]
	if !ok {
		t.Fatal("packed nullable column missing")
	}
	if packed.Rows != len(values) {
		t.Fatalf("packed rows = %d, want %d", packed.Rows, len(values))
	}
	if !reflect.DeepEqual(packed.Values, []interface{}{int64(7), "ready", float64(3.5), true}) {
		t.Fatalf("packed values = %#v", packed.Values)
	}
	for row, want := range values {
		got, ok := batch.Value("value", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(value, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
	if got := batch.FieldRows("value"); got != len(values) {
		t.Fatalf("FieldRows(value) = %d, want %d", got, len(values))
	}
}

func TestColumnarBatchPackNullableColumnsPreservesSmallLegacyColumns(t *testing.T) {
	values := make([]interface{}, 100)
	for index := range values {
		values[index] = int64(index)
	}
	values[50] = nil
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}

	batch.PackNullableColumns()

	if !reflect.DeepEqual(batch.Columns["value"], values) {
		t.Fatalf("legacy values changed: %#v", batch.Columns["value"])
	}
	if len(batch.PackedColumns) != 0 {
		t.Fatalf("small-savings packed columns = %#v", batch.PackedColumns)
	}
}

func TestColumnarPackedColumnRankBoundaries(t *testing.T) {
	values := make([]interface{}, 65)
	values[0] = "first"
	values[63] = "before-boundary"
	values[64] = "after-boundary"
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNullableColumns()

	column, ok := batch.PackedColumns["value"]
	if !ok {
		t.Fatal("boundary column was not packed")
	}
	for row, want := range values {
		got, valid := batch.Value("value", row)
		if !valid || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(value, %d) = %#v, %v; want %#v, true", row, got, valid, want)
		}
	}
	if got := len(column.Validity); got != 9 {
		t.Fatalf("validity bytes = %d, want 9", got)
	}
	if got := len(column.Ranks); got != 10 {
		t.Fatalf("rank entries = %d, want 10", got)
	}
}

func TestColumnarPackedColumnRejectsMalformedMetadata(t *testing.T) {
	validRanks := make([]uint32, 2)
	validRanks[1] = 1
	valid := ColumnarPackedColumn{Values: []interface{}{"ok"}, Validity: []byte{1}, Ranks: validRanks, Rows: 8}
	if got := valid.RowCount(); got != 8 {
		t.Fatalf("valid RowCount = %d, want 8", got)
	}
	descendingRanks := []uint32{1, 0}
	for name, column := range map[string]ColumnarPackedColumn{
		"short bitmap":  {Values: []interface{}{"ok"}, Validity: nil, Ranks: []uint32{0, 1}, Rows: 8},
		"short ranks":   {Values: []interface{}{"ok"}, Validity: []byte{1}, Ranks: []uint32{0}, Rows: 8},
		"wrong total":   {Values: []interface{}{"ok"}, Validity: []byte{1}, Ranks: []uint32{0, 2}, Rows: 8},
		"descending":    {Values: []interface{}{"ok"}, Validity: []byte{1}, Ranks: descendingRanks, Rows: 8},
		"trailing bits": {Values: []interface{}{"ok"}, Validity: []byte{0x81}, Ranks: []uint32{0, 2}, Rows: 7},
	} {
		t.Run(name, func(t *testing.T) {
			if got := column.RowCount(); got != 0 {
				t.Fatalf("RowCount = %d, want 0", got)
			}
			if got, ok := column.Value(0); ok || got != nil {
				t.Fatalf("Value(0) = %#v, %v; want nil, false", got, ok)
			}
		})
	}
}

func TestPackedNullableColumnSQLFilterAndVerticalMerge(t *testing.T) {
	first := ColumnarBatch{Columns: map[string][]interface{}{"value": {"ready", nil, "other", nil}}, Rows: 4}
	first.PackNullableColumns()
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value = 'ready'", packedNullableQueryResolver{batch: first}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"value": "ready"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered rows = %#v, want %#v", result.Rows, want)
	}

	second := ColumnarBatch{Columns: map[string][]interface{}{"value": {nil, "tail"}}, Rows: 2}
	second.PackNullableColumns()
	merged, err := MergeColumnarParts([]ColumnarMergePart{ColumnarBatchPart{Batch: first}, ColumnarBatchPart{Batch: second}}, []string{"value"})
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []interface{}{"ready", nil, "other", nil, nil, "tail"}
	for row, want := range wantValues {
		got, ok := merged.Value("value", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("merged Value(value, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
}

var nullablePackedBenchmarkSink int

func newNullablePackedBenchmarkBatch(rows int) ColumnarBatch {
	values := make([]interface{}, rows)
	for row := range values {
		if row&3 == 0 {
			values[row] = "ready"
		}
	}
	return ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: rows}
}

func nullablePackedBenchmarkLayoutBytes(batch ColumnarBatch) int {
	if values, ok := batch.Columns["value"]; ok {
		return len(values) * 16
	}
	column := batch.PackedColumns["value"]
	return len(column.Values)*16 + len(column.Validity) + len(column.Ranks)*4
}

func BenchmarkColumnarNullablePackedLookup(b *testing.B) {
	for _, test := range []struct {
		name   string
		packed bool
	}{
		{name: "legacy", packed: false},
		{name: "packed", packed: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := newNullablePackedBenchmarkBatch(4096)
			if test.packed {
				batch.PackNullableColumns()
			}
			b.ResetTimer()
			b.ReportMetric(float64(nullablePackedBenchmarkLayoutBytes(batch)), "layout-bytes/op")
			checksum := 0
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < batch.Rows; row++ {
					value, ok := batch.Value("value", row)
					if ok && value != nil {
						checksum += len(value.(string))
					}
				}
			}
			nullablePackedBenchmarkSink = checksum
		})
	}
}
