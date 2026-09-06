package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

func TestColumnarBatchSelectsDictionaryForWideRepeatedValues(t *testing.T) {
	wide := func(index int) string {
		return strings.Repeat("payload-", 24) + string(rune('0'+index))
	}
	values := []interface{}{wide(0), wide(1), wide(2), wide(3), wide(0), wide(1)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"payload": values}, Rows: len(values)}

	batch.EncodeRepeatedStrings()
	dictionary, ok := batch.Dictionaries["payload"]
	if !ok {
		t.Fatal("wide repeated column was not dictionary encoded")
	}
	if want := []string{wide(0), wide(1), wide(2), wide(3)}; !reflect.DeepEqual(dictionary.Values, want) {
		t.Fatalf("dictionary values = %#v, want %#v", dictionary.Values, want)
	}
	for index, want := range values {
		got, ok := batch.Value("payload", index)
		if !ok || got != want {
			t.Fatalf("Value(payload, %d) = %#v/%v, want %#v/true", index, got, ok, want)
		}
	}
}

func TestColumnarBatchKeepsNarrowUniqueValuesPlain(t *testing.T) {
	values := []interface{}{"a", "b", "c", "d", "e", "f"}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}

	batch.EncodeRepeatedStrings()
	if _, ok := batch.Dictionaries["value"]; ok {
		t.Fatal("narrow unique column was dictionary encoded")
	}
	if !reflect.DeepEqual(batch.Columns["value"], values) {
		t.Fatalf("plain values = %#v, want %#v", batch.Columns["value"], values)
	}
}

func BenchmarkColumnarBatchLayoutSelection(b *testing.B) {
	wideValues := make([]interface{}, 1024)
	for index := range wideValues {
		wideValues[index] = strings.Repeat("payload-", 16) + string([]byte{byte(index >> 8), byte(index)})
	}
	for _, benchmark := range []struct {
		name   string
		values []interface{}
	}{
		{name: "wide_700_unique", values: wideValues},
		{name: "narrow_unique", values: []interface{}{"a", "b", "c", "d", "e", "f"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				values := append([]interface{}(nil), benchmark.values...)
				batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
				batch.EncodeRepeatedStrings()
			}
		})
	}
}
