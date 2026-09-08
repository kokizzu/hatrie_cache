package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type packedBoolQueryResolver struct {
	batch ColumnarBatch
}

func (resolver packedBoolQueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a packed boolean column")
}

func (resolver packedBoolQueryResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestColumnarBatchPackBooleanColumnsRoundTrip(t *testing.T) {
	values := []interface{}{true, false, nil, true, nil, false}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"active": values},
		Rows:    len(values),
	}

	batch.PackBooleanColumns()

	if _, ok := batch.Columns["active"]; ok {
		t.Fatal("packed boolean column still has a legacy column")
	}
	packed, ok := batch.BoolColumns["active"]
	if !ok {
		t.Fatal("packed boolean column missing")
	}
	for row, want := range values {
		got, ok := batch.Value("active", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(active, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
	if got := batch.FieldRows("active"); got != len(values) {
		t.Fatalf("FieldRows(active) = %d, want %d", got, len(values))
	}
	if packed.Rows != len(values) {
		t.Fatalf("packed rows = %d, want %d", packed.Rows, len(values))
	}
}

func TestColumnarBatchPackBooleanColumnsPreservesMixedInput(t *testing.T) {
	values := []interface{}{true, nil, "true", false}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}

	batch.PackBooleanColumns()

	if !reflect.DeepEqual(batch.Columns["active"], values) {
		t.Fatalf("mixed values changed: %#v", batch.Columns["active"])
	}
	if len(batch.BoolColumns) != 0 {
		t.Fatalf("mixed input was packed: %#v", batch.BoolColumns)
	}
}

func TestColumnarBatchPackBooleanColumnsUsesOneBitmapForAllValidValues(t *testing.T) {
	values := []interface{}{true, false, true, true, false, false, true, false, true}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}

	batch.PackBooleanColumns()

	column, ok := batch.BoolColumns["active"]
	if !ok {
		t.Fatal("all-valid boolean column was not packed")
	}
	if column.Validity != nil {
		t.Fatalf("all-valid validity bitmap = %#v, want nil", column.Validity)
	}
	if len(column.Bits) != 2 {
		t.Fatalf("value bitmap bytes = %d, want 2", len(column.Bits))
	}
	for row, want := range values {
		got, valid := batch.Value("active", row)
		if !valid || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(active, %d) = %#v, %v; want %#v, true", row, got, valid, want)
		}
	}
}

func TestColumnarBoolColumnRejectsMalformedMetadata(t *testing.T) {
	for name, column := range map[string]ColumnarBoolColumn{
		"short bits":     {Bits: nil, Rows: 8},
		"short validity": {Bits: []byte{1}, Validity: []byte{}, Rows: 8},
		"trailing bits":  {Bits: []byte{0x81}, Rows: 7},
		"valid trailing": {Bits: []byte{1}, Validity: []byte{0x81}, Rows: 7},
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

func TestPackedBooleanColumnSQLFilterAndVerticalMerge(t *testing.T) {
	first := ColumnarBatch{Columns: map[string][]interface{}{"active": {true, nil, false, true}}, Rows: 4}
	first.PackBooleanColumns()
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT active FROM CACHE('items') WHERE active = true", packedBoolQueryResolver{batch: first}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"active": true}, {"active": true}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered rows = %#v, want %#v", result.Rows, want)
	}

	second := ColumnarBatch{Columns: map[string][]interface{}{"active": {nil, false}}, Rows: 2}
	second.PackBooleanColumns()
	merged, err := MergeColumnarParts([]ColumnarMergePart{ColumnarBatchPart{Batch: first}, ColumnarBatchPart{Batch: second}}, []string{"active"})
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []interface{}{true, nil, false, true, nil, false}
	for row, want := range wantValues {
		got, ok := merged.Value("active", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("merged Value(active, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
}

var boolPackedBenchmarkSink int

func newBoolPackedBenchmarkBatch(rows int) ColumnarBatch {
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = row&1 == 0
	}
	return ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: rows}
}

func boolPackedBenchmarkLayoutBytes(batch ColumnarBatch) int {
	if values, ok := batch.Columns["active"]; ok {
		return len(values) * 16
	}
	column := batch.BoolColumns["active"]
	return len(column.Bits) + len(column.Validity)
}

func BenchmarkColumnarBooleanPackedLookup(b *testing.B) {
	for _, test := range []struct {
		name   string
		packed bool
	}{
		{name: "legacy", packed: false},
		{name: "packed", packed: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := newBoolPackedBenchmarkBatch(4096)
			if test.packed {
				batch.PackBooleanColumns()
			}
			b.ResetTimer()
			b.ReportMetric(float64(boolPackedBenchmarkLayoutBytes(batch)), "layout-bytes/op")
			checksum := 0
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < batch.Rows; row++ {
					value, ok := batch.Value("active", row)
					if ok && value.(bool) {
						checksum++
					}
				}
			}
			boolPackedBenchmarkSink = checksum
		})
	}
}
