package hatSql

import (
	"fmt"
	"testing"
)

var mz026AdaptiveArrangementBenchmarkSink int

func BenchmarkMZ026AdaptiveSortedArrangementRawBuild(b *testing.B) {
	benchmarkMZ026AdaptiveSortedArrangementBuild(b, 16, false)
}

func BenchmarkMZ026AdaptiveSortedArrangementLowCardinalityBuild(b *testing.B) {
	benchmarkMZ026AdaptiveSortedArrangementBuild(b, 16, true)
}

func BenchmarkMZ026AdaptiveSortedArrangementHighCardinalityRawBuild(b *testing.B) {
	benchmarkMZ026AdaptiveSortedArrangementBuild(b, 4096, false)
}

func BenchmarkMZ026AdaptiveSortedArrangementHighCardinalityBuild(b *testing.B) {
	benchmarkMZ026AdaptiveSortedArrangementBuild(b, 4096, true)
}

func benchmarkMZ026AdaptiveSortedArrangementBuild(b *testing.B, distinct int, adaptive bool) {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "mz026_adaptive_arrangement_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := range 4096 {
		if _, err := table.Upsert(fmt.Sprintf("key-%05d", index), []TypedTableValue{
			TypedString(fmt.Sprintf("team-%05d", index%distinct)), TypedInt64(int64(index)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	sample, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
		Field:              "team",
		DictionaryAdaptive: adaptive,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
			Field:              "team",
			DictionaryAdaptive: adaptive,
		})
		if err != nil {
			b.Fatal(err)
		}
		mz026AdaptiveArrangementBenchmarkSink += len(arrangement.entries)
	}
	b.ReportMetric(float64(typedTableSortedArrangementRetainedSortStringBytes(sample)), "retained_sort_string_bytes")
}
