package hatSql

import "testing"

var tr019ColumnarValueSink interface{}

func TestTR019ColumnarValuePreservesPhysicalPrecedence(t *testing.T) {
	plain := []interface{}{"plain"}
	packed := ColumnarPackedColumn{Rows: 1, Validity: []byte{1}, Ranks: []uint32{0, 1}, Values: []interface{}{"packed"}}
	batch := ColumnarBatch{
		Columns:       map[string][]interface{}{"value": plain},
		PackedColumns: map[string]ColumnarPackedColumn{"value": packed},
		Rows:          1,
	}
	if value, ok := batch.Value("value", 0); !ok || value != "packed" {
		t.Fatalf("specialized value = %#v, %t, want packed, true", value, ok)
	}

	plainBatch := ColumnarBatch{Columns: map[string][]interface{}{"value": plain}, Rows: 1}
	if value, ok := plainBatch.Value("value", 0); !ok || value != "plain" {
		t.Fatalf("plain value = %#v, %t, want plain, true", value, ok)
	}
	if value, ok := plainBatch.Value("missing", 0); ok || value != nil {
		t.Fatalf("missing value = %#v, %t, want nil, false", value, ok)
	}
}

func tr019ColumnarPlainBatch() ColumnarBatch {
	values := make([]interface{}, 1024)
	for index := range values {
		values[index] = int64(index)
	}
	return ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
}

func tr019ColumnarValueBaseline(batch ColumnarBatch, field string, row int) (interface{}, bool) {
	if row < 0 {
		return nil, false
	}
	if dictionary, ok := batch.Dictionaries[field]; ok {
		code, ok := dictionary.CodeAt(row)
		if !ok {
			return nil, false
		}
		value, ok := dictionary.ValueAt(code)
		if !ok {
			return nil, false
		}
		return value, true
	}
	if column, ok := batch.PackedColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.BoolColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.NumericColumns[field]; ok {
		return column.Value(row)
	}
	if values, ok := batch.Columns[field]; ok {
		if row >= len(values) {
			return nil, false
		}
		return values[row], true
	}
	if column, ok := batch.ListColumns[field]; ok {
		return column.Value(row)
	}
	if column, ok := batch.NestedColumns[field]; ok {
		return column.Value(row)
	}
	return nil, false
}

func BenchmarkTR019ColumnarValueBaseline(b *testing.B) {
	batch := tr019ColumnarPlainBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := tr019ColumnarValueBaseline(batch, "value", index&1023)
		if !ok {
			b.Fatal("baseline lookup failed")
		}
		tr019ColumnarValueSink = value
	}
}

func BenchmarkTR019ColumnarValue(b *testing.B) {
	batch := tr019ColumnarPlainBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := batch.Value("value", index&1023)
		if !ok {
			b.Fatal("lookup failed")
		}
		tr019ColumnarValueSink = value
	}
}
