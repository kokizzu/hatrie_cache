package hatSql

import (
	"reflect"
	"strconv"
	"testing"
)

func TestCHU55TypedTableInt64DictionaryStorage(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "status", Kind: TypedTableInt64, DictionaryEncoded: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !table.columns[0].dictionary {
		t.Fatal("int64 DictionaryEncoded did not enable dictionary storage")
	}
	for _, row := range []struct {
		key    string
		status TypedTableValue
	}{{"a", TypedInt64(1)}, {"b", TypedInt64(2)}, {"c", TypedInt64(1)}, {"d", TypedNull()}} {
		if _, err := table.Upsert(row.key, []TypedTableValue{row.status}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	rows := table.Rows()
	values := make(map[interface{}]int)
	for _, row := range rows {
		values[row["status"]]++
	}
	if len(rows) != 3 || values[nil] != 1 || values[int64(1)] != 1 || values[int64(3)] != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if len(table.columns[0].int64s) != 0 || len(table.columns[0].dictionaryCodes) != len(rows) {
		t.Fatalf("int64 dictionary storage = %#v", table.columns[0])
	}
}

func TestCHU55TypedTableInt64DictionaryColumnarAppend(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "status", Kind: TypedTableInt64, DictionaryEncoded: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.AppendColumnar([]string{"a", "b", "c", "d"}, ColumnarBatch{
		Rows:    4,
		Columns: map[string][]interface{}{"status": {int64(1), int64(2), nil, int64(1)}},
	}); err != nil {
		t.Fatal(err)
	}
	if len(table.columns[0].int64s) != 0 || len(table.columns[0].dictionaryCodes) != 4 {
		t.Fatalf("columnar dictionary storage = %#v", table.columns[0])
	}
	table.mu.RLock()
	batch := table.columnarBatchLocked([]string{"status"})
	table.mu.RUnlock()
	if got, want := batch.Columns["status"], []interface{}{int64(1), int64(2), nil, int64(1)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("columnar values = %#v, want %#v", got, want)
	}
}

func TestCHU55TypedTableInt64DictionaryHistogram(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "status", Kind: TypedTableInt64, DictionaryEncoded: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]TypedTableValue{
		"one":   TypedInt64(1),
		"two":   TypedInt64(2),
		"three": TypedInt64(1),
		"null":  TypedNull(),
	} {
		if _, err := table.Upsert(key, []TypedTableValue{value}); err != nil {
			t.Fatal(err)
		}
	}
	histogram, err := table.Histogram("status", TypedTableHistogramOptions{Bins: 2})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.RowCount != 4 || histogram.NullCount != 1 || histogram.ValueCount != 3 || histogram.Min != TypedInt64(1) || histogram.Max != TypedInt64(2) {
		t.Fatalf("histogram = %#v", histogram)
	}
}

func BenchmarkCHU55TypedTableInt64Storage(b *testing.B) {
	for _, benchmark := range []struct {
		name        string
		dictionary  bool
		cardinality int
	}{{"plain-repeated", false, 8}, {"dictionary-repeated", true, 8}, {"plain-unique", false, 2048}, {"dictionary-unique", true, 2048}} {
		b.Run(benchmark.name, func(b *testing.B) {
			keys := make([]string, 2048)
			values := make([]TypedTableValue, len(keys))
			for index := range keys {
				keys[index] = strconv.Itoa(index)
				values[index] = TypedInt64(int64(index % benchmark.cardinality))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				table, err := NewTypedTable(TypedTableSchema{
					Name: "events",
					Columns: []TypedTableColumn{{
						Name:              "status",
						Kind:              TypedTableInt64,
						DictionaryEncoded: benchmark.dictionary,
					}},
				})
				if err != nil {
					b.Fatal(err)
				}
				for index, key := range keys {
					if _, err := table.Upsert(key, values[index:index+1]); err != nil {
						b.Fatal(err)
					}
				}
				if got := len(table.Rows()); got != len(keys) {
					b.Fatalf("rows = %d, want %d", got, len(keys))
				}
				b.ReportMetric(float64(chu55Int64StoragePayloadBytes(table)), "payload-bytes/op")
			}
		})
	}
}

var chu55TypedTableBenchmarkSink *TypedTable

func BenchmarkCHU55TypedTableInt64Upsert(b *testing.B) {
	for _, benchmark := range []struct {
		name        string
		dictionary  bool
		cardinality int
	}{{"plain-repeated", false, 8}, {"dictionary-repeated", true, 8}, {"plain-unique", false, 2048}, {"dictionary-unique", true, 2048}} {
		b.Run(benchmark.name, func(b *testing.B) {
			keys := make([]string, 2048)
			values := make([]TypedTableValue, len(keys))
			for index := range keys {
				keys[index] = strconv.Itoa(index)
				values[index] = TypedInt64(int64(index % benchmark.cardinality))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				table, err := NewTypedTable(TypedTableSchema{
					Name: "events",
					Columns: []TypedTableColumn{{
						Name:              "status",
						Kind:              TypedTableInt64,
						DictionaryEncoded: benchmark.dictionary,
					}},
				})
				if err != nil {
					b.Fatal(err)
				}
				for index, key := range keys {
					if _, err := table.Upsert(key, values[index:index+1]); err != nil {
						b.Fatal(err)
					}
				}
				chu55TypedTableBenchmarkSink = table
				b.ReportMetric(float64(chu55Int64StoragePayloadBytes(table)), "payload-bytes/op")
			}
		})
	}
}

func chu55Int64StoragePayloadBytes(table *TypedTable) int {
	storage := &table.columns[0]
	bytes := len(storage.valid)
	if storage.dictionary {
		return bytes + len(storage.dictionaryCodes)*4 + len(storage.dictionaryInt64Values)*8
	}
	return bytes + len(storage.int64s)*8
}
