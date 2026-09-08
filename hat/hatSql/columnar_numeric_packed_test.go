package hatSql

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

type packedNumericQueryResolver struct {
	batch ColumnarBatch
}

func (resolver packedNumericQueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a packed numeric column")
}

func (resolver packedNumericQueryResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestColumnarBatchPackNumericColumnsRoundTrip(t *testing.T) {
	values := []interface{}{int64(7), nil, int64(-3), int64(99)}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": values},
		Rows:    len(values),
	}

	batch.PackNumericColumns()

	if _, ok := batch.Columns["value"]; ok {
		t.Fatal("packed numeric column still has a legacy column")
	}
	if _, ok := batch.NumericColumns["value"]; !ok {
		t.Fatal("packed numeric column missing")
	}
	for row, want := range values {
		got, ok := batch.Value("value", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(value, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
}

func TestColumnarBatchPackNumericColumnsPreservesFloat64Bits(t *testing.T) {
	values := []interface{}{
		math.Copysign(0, -1),
		math.Inf(1),
		math.Float64frombits(0x7ff8000000000042),
		nil,
		-12.5,
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": values},
		Rows:    len(values),
	}

	batch.PackNumericColumns()
	column, ok := batch.NumericColumns["value"]
	if !ok || column.Kind != ColumnarNumericFloat64 {
		t.Fatalf("numeric column = %#v, want float64 packed column", column)
	}
	if got := len(column.Data); got != len(values)*8 {
		t.Fatalf("data bytes = %d, want %d", got, len(values)*8)
	}
	if got := len(column.Validity); got != 1 {
		t.Fatalf("validity bytes = %d, want 1", got)
	}
	for row, want := range values {
		got, valid := batch.Value("value", row)
		if !valid {
			t.Fatalf("Value(value, %d) was invalid", row)
		}
		if want == nil {
			if got != nil {
				t.Fatalf("Value(value, %d) = %#v, want nil", row, got)
			}
			continue
		}
		if got == nil || math.Float64bits(got.(float64)) != math.Float64bits(want.(float64)) {
			t.Fatalf("Value(value, %d) = %#v, want exact bits of %#v", row, got, want)
		}
	}
}

func TestColumnarBatchPackNumericColumnsPreservesUnsupportedAndMixedInput(t *testing.T) {
	cases := map[string][]interface{}{
		"int":        {int(1), int(2)},
		"mixed":      {int64(1), float64(2)},
		"all nil":    {nil, nil, nil},
		"string":     {"1", "2"},
		"bool mixed": {int64(1), true},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
			batch.PackNumericColumns()
			if _, ok := batch.NumericColumns["value"]; ok {
				t.Fatalf("unsupported input was packed: %#v", values)
			}
			if !reflect.DeepEqual(batch.Columns["value"], values) {
				t.Fatalf("legacy values = %#v, want %#v", batch.Columns["value"], values)
			}
		})
	}
}

func TestColumnarNumericColumnBoundariesAndMalformedMetadata(t *testing.T) {
	values := make([]interface{}, 65)
	values[0] = int64(7)
	values[63] = int64(-3)
	values[64] = int64(99)
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	column, ok := batch.NumericColumns["value"]
	if !ok {
		t.Fatal("boundary column was not packed")
	}
	if got := len(column.Data); got != 65*8 {
		t.Fatalf("data bytes = %d, want %d", got, 65*8)
	}
	for row, want := range values {
		got, valid := batch.Value("value", row)
		if !valid || !reflect.DeepEqual(got, want) {
			t.Fatalf("Value(value, %d) = %#v, %v; want %#v, true", row, got, valid, want)
		}
	}

	valid := ColumnarNumericColumn{Kind: ColumnarNumericInt64, Data: make([]byte, 16), Rows: 2}
	if got := valid.RowCount(); got != 2 {
		t.Fatalf("valid RowCount = %d, want 2", got)
	}
	cases := map[string]ColumnarNumericColumn{
		"short data":     {Kind: ColumnarNumericInt64, Data: make([]byte, 8), Rows: 2},
		"extra data":     {Kind: ColumnarNumericInt64, Data: make([]byte, 24), Rows: 2},
		"short validity": {Kind: ColumnarNumericInt64, Data: make([]byte, 16), Validity: []byte{}, Rows: 2},
		"bad kind":       {Kind: 99, Data: make([]byte, 16), Rows: 2},
		"trailing bits":  {Kind: ColumnarNumericInt64, Data: make([]byte, 72), Validity: []byte{0x81}, Rows: 7},
	}
	for name, malformed := range cases {
		t.Run(name, func(t *testing.T) {
			if got := malformed.RowCount(); got != 0 {
				t.Fatalf("RowCount = %d, want 0", got)
			}
			if got, ok := malformed.Value(0); ok || got != nil {
				t.Fatalf("Value(0) = %#v, %v; want nil, false", got, ok)
			}
		})
	}
}

func TestPackedNumericColumnSQLFilterAndVerticalMerge(t *testing.T) {
	first := ColumnarBatch{Columns: map[string][]interface{}{"value": {int64(1), nil, int64(3)}}, Rows: 3}
	first.PackNumericColumns()
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value >= 2", packedNumericQueryResolver{batch: first}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"value": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered rows = %#v, want %#v", result.Rows, want)
	}

	second := ColumnarBatch{Columns: map[string][]interface{}{"value": {int64(5), nil}}, Rows: 2}
	second.PackNumericColumns()
	merged, err := MergeColumnarParts([]ColumnarMergePart{ColumnarBatchPart{Batch: first}, ColumnarBatchPart{Batch: second}}, []string{"value"})
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []interface{}{int64(1), nil, int64(3), int64(5), nil}
	for row, want := range wantValues {
		got, ok := merged.Value("value", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("merged Value(value, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
}

var numericPackedBenchmarkSink int64

func newNumericPackedBenchmarkBatch(rows int) ColumnarBatch {
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = int64(row % 97)
	}
	return ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: rows}
}

func numericPackedBenchmarkLayoutBytes(batch ColumnarBatch) int {
	if values, ok := batch.Columns["value"]; ok {
		return len(values) * 16
	}
	column := batch.NumericColumns["value"]
	return len(column.Data) + len(column.Validity)
}

func BenchmarkColumnarNumericPackedLookup(b *testing.B) {
	for _, test := range []struct {
		name   string
		packed bool
	}{
		{name: "legacy", packed: false},
		{name: "packed", packed: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := newNumericPackedBenchmarkBatch(4096)
			if test.packed {
				batch.PackNumericColumns()
			}
			b.ResetTimer()
			b.ReportMetric(float64(numericPackedBenchmarkLayoutBytes(batch)), "layout-bytes/op")
			checksum := int64(0)
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < batch.Rows; row++ {
					value, ok := batch.Value("value", row)
					if ok {
						checksum += value.(int64)
					}
				}
			}
			numericPackedBenchmarkSink = checksum
		})
	}
}
