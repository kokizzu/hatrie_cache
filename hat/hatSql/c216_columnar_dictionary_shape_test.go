package hatSql

import (
	"fmt"
	"testing"
)

func TestC216ColumnarDictionarySelectionUsesLookupShapeNearMemoryBreakEven(t *testing.T) {
	values := c216BreakEvenStringValues()
	tests := []struct {
		name      string
		shape     ColumnarDictionaryLookupShape
		wantDict  bool
		wantPlain bool
	}{
		{name: "automatic", shape: ColumnarDictionaryLookupAutomatic, wantDict: true},
		{name: "equality", shape: ColumnarDictionaryLookupEquality, wantDict: true},
		{name: "grouping", shape: ColumnarDictionaryLookupGrouping, wantDict: true},
		{name: "ordering", shape: ColumnarDictionaryLookupOrdering, wantPlain: true},
		{name: "projection", shape: ColumnarDictionaryLookupProjection, wantPlain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := ColumnarBatch{Columns: map[string][]interface{}{"value": append([]interface{}(nil), values...)}, Rows: len(values)}
			batch.EncodeRepeatedStringsForLookup(test.shape)
			_, dictionary := batch.Dictionaries["value"]
			_, plain := batch.Columns["value"]
			if dictionary != test.wantDict || plain != test.wantPlain {
				t.Fatalf("shape %v produced dictionary=%t plain=%t, want dictionary=%t plain=%t", test.shape, dictionary, plain, test.wantDict, test.wantPlain)
			}
		})
	}
}

func TestC216ColumnarDictionarySelectionAllowsExactEqualityBreakEven(t *testing.T) {
	values := []interface{}{"", "a", "b", ""}
	for _, test := range []struct {
		name     string
		shape    ColumnarDictionaryLookupShape
		wantDict bool
	}{
		{name: "automatic", shape: ColumnarDictionaryLookupAutomatic, wantDict: false},
		{name: "equality", shape: ColumnarDictionaryLookupEquality, wantDict: true},
		{name: "grouping", shape: ColumnarDictionaryLookupGrouping, wantDict: true},
		{name: "ordering", shape: ColumnarDictionaryLookupOrdering, wantDict: false},
		{name: "projection", shape: ColumnarDictionaryLookupProjection, wantDict: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			batch := ColumnarBatch{
				Columns: map[string][]interface{}{"value": append([]interface{}(nil), values...)},
				Rows:    len(values),
			}
			batch.EncodeRepeatedStringsForLookup(test.shape)
			_, gotDict := batch.Dictionaries["value"]
			if gotDict != test.wantDict {
				t.Fatalf("dictionary selected = %v, want %v", gotDict, test.wantDict)
			}
		})
	}
}

func TestC216ColumnarDictionarySelectionPreservesValues(t *testing.T) {
	values := c216BreakEvenStringValues()
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": append([]interface{}(nil), values...)}, Rows: len(values)}
	batch.EncodeRepeatedStringsForLookup(ColumnarDictionaryLookupGrouping)
	for index, want := range values {
		if got, ok := batch.Value("value", index); !ok || got != want {
			t.Fatalf("Value(value, %d) = %#v/%t, want %#v/true", index, got, ok, want)
		}
	}
}

func c216BreakEvenStringValues() []interface{} {
	values := make([]interface{}, 1024)
	for index := range values {
		values[index] = fmt.Sprintf("%04d", index%768)
	}
	return values
}

var c216ColumnarBatchSink ColumnarBatch
var c216ColumnarValueSink int

func BenchmarkC216ColumnarDictionarySelection(b *testing.B) {
	values := c216BreakEvenStringValues()
	for _, test := range []struct {
		name  string
		shape ColumnarDictionaryLookupShape
	}{
		{name: "automatic", shape: ColumnarDictionaryLookupAutomatic},
		{name: "equality", shape: ColumnarDictionaryLookupEquality},
		{name: "grouping", shape: ColumnarDictionaryLookupGrouping},
		{name: "ordering", shape: ColumnarDictionaryLookupOrdering},
		{name: "projection", shape: ColumnarDictionaryLookupProjection},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				batch := ColumnarBatch{
					Columns: map[string][]interface{}{"value": append([]interface{}(nil), values...)},
					Rows:    len(values),
				}
				batch.EncodeRepeatedStringsForLookup(test.shape)
				c216ColumnarBatchSink = batch
			}
		})
	}
}

func BenchmarkC216ColumnarDictionaryValueRead(b *testing.B) {
	values := c216BreakEvenStringValues()
	for _, test := range []struct {
		name  string
		shape ColumnarDictionaryLookupShape
	}{
		{name: "automatic", shape: ColumnarDictionaryLookupAutomatic},
		{name: "equality", shape: ColumnarDictionaryLookupEquality},
		{name: "grouping", shape: ColumnarDictionaryLookupGrouping},
		{name: "ordering", shape: ColumnarDictionaryLookupOrdering},
		{name: "projection", shape: ColumnarDictionaryLookupProjection},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := ColumnarBatch{
				Columns: map[string][]interface{}{"value": append([]interface{}(nil), values...)},
				Rows:    len(values),
			}
			batch.EncodeRepeatedStringsForLookup(test.shape)
			b.ReportAllocs()
			b.ResetTimer()
			checksum := 0
			for range b.N {
				for row := 0; row < batch.Rows; row++ {
					value, ok := batch.Value("value", row)
					if !ok {
						b.Fatalf("Value(value, %d) unavailable", row)
					}
					checksum += len(value.(string))
				}
			}
			c216ColumnarValueSink = checksum
		})
	}
}
