package hatSql

import (
	"fmt"
	"testing"
)

var typedTableSortedArrangementDictionaryBenchmarkSink int

func BenchmarkTypedTableSortedArrangementLegacyRepeatedStringRowsPage(b *testing.B) {
	benchmarkTypedTableSortedArrangementRepeatedStringRowsPage(b, false)
}

func BenchmarkTypedTableSortedArrangementDictionaryRepeatedStringRowsPage(b *testing.B) {
	benchmarkTypedTableSortedArrangementRepeatedStringRowsPage(b, true)
}

func BenchmarkTypedTableSortedArrangementLegacyBuild(b *testing.B) {
	benchmarkTypedTableSortedArrangementBuild(b, false)
}

func BenchmarkTypedTableSortedArrangementDictionaryBuild(b *testing.B) {
	benchmarkTypedTableSortedArrangementBuild(b, true)
}

func BenchmarkTypedTableSortedArrangementSingleOrderBuild(b *testing.B) {
	benchmarkTypedTableSortedArrangementOrderBuild(b, false)
}

func BenchmarkTypedTableSortedArrangementCompositeOrderBuild(b *testing.B) {
	benchmarkTypedTableSortedArrangementOrderBuild(b, true)
}

func benchmarkTypedTableSortedArrangementBuild(b *testing.B, dictionaryEncoded bool) {
	table := newTypedTableSortedArrangementDictionaryBenchmarkTable(b)
	b.ResetTimer()
	for range b.N {
		arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
			Field:             "team",
			DictionaryEncoded: dictionaryEncoded,
		})
		if err != nil {
			b.Fatal(err)
		}
		typedTableSortedArrangementDictionaryBenchmarkSink += len(arrangement.entries)
	}
}

func benchmarkTypedTableSortedArrangementOrderBuild(b *testing.B, composite bool) {
	table := newTypedTableSortedArrangementDictionaryBenchmarkTable(b)
	definition := TypedTableSortedArrangementDefinition{Field: "team"}
	if composite {
		definition = TypedTableSortedArrangementDefinition{
			OrderBy: []TypedTableSortedArrangementOrder{
				{Field: "team"},
				{Field: "score", Descending: true},
			},
		}
	}
	b.ResetTimer()
	for range b.N {
		arrangement, err := NewTypedTableSortedArrangement(table, definition)
		if err != nil {
			b.Fatal(err)
		}
		typedTableSortedArrangementDictionaryBenchmarkSink += len(arrangement.entries)
	}
}

func benchmarkTypedTableSortedArrangementRepeatedStringRowsPage(b *testing.B, dictionaryEncoded bool) {
	table := newTypedTableSortedArrangementDictionaryBenchmarkTable(b)
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
		Field:             "team",
		DictionaryEncoded: dictionaryEncoded,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(typedTableSortedArrangementRetainedSortStringBytes(arrangement)), "retained_sort_string_bytes")
	for range b.N {
		typedTableSortedArrangementDictionaryBenchmarkSink += len(arrangement.RowsPage(0, 32))
	}
}

func newTypedTableSortedArrangementDictionaryBenchmarkTable(b *testing.B) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "sorted_dictionary_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := range 4096 {
		team := string(append([]byte(nil), []byte(fmt.Sprintf("team-%02d", index%16))...))
		if _, err := table.Upsert(fmt.Sprintf("key-%05d", index), []TypedTableValue{TypedString(team), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}

func typedTableSortedArrangementRetainedSortStringBytes(arrangement *TypedTableSortedArrangement) int {
	if arrangement == nil {
		return 0
	}
	if arrangement.dictionary != nil {
		bytes := 0
		for _, value := range arrangement.dictionary.values {
			bytes += len(value)
		}
		return bytes
	}
	bytes := 0
	for _, row := range arrangement.entries {
		if arrangement.field >= 0 && arrangement.field < len(row.Values) {
			bytes += len(row.Values[arrangement.field].String)
		}
	}
	return bytes
}
