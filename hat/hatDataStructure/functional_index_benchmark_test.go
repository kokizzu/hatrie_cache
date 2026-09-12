package hatDataStructure

import (
	"fmt"
	"runtime"
	"testing"
)

type functionalIndexBenchmarkRow struct {
	ID    uint64
	Key   string
	Value int
}

func BenchmarkFunctionalIndexLookup(b *testing.B) {
	const (
		rowCount   = 10000
		distinct   = 1000
		targetKey  = "key-042"
		scratchCap = rowCount / distinct * 2
	)

	rows := make([]functionalIndexBenchmarkRow, rowCount)
	index, err := NewFunctionalIndex[functionalIndexBenchmarkRow, string](func(row functionalIndexBenchmarkRow) string {
		return row.Key
	}, rowCount)
	if err != nil {
		b.Fatal(err)
	}
	for i := range rows {
		rows[i] = functionalIndexBenchmarkRow{
			ID:    uint64(i + 1),
			Key:   fmt.Sprintf("key-%03d", i%distinct),
			Value: i * 3,
		}
		if err := index.Upsert(rows[i].ID, rows[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("scan", func(b *testing.B) {
		b.ReportAllocs()
		matches := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for _, row := range rows {
				if row.Key == targetKey {
					count++
				}
			}
			matches += count
		}
		b.StopTimer()
		b.ReportMetric(float64(matches)/float64(b.N), "matches/op")
		runtime.KeepAlive(rows)
	})

	b.Run("index", func(b *testing.B) {
		b.ReportAllocs()
		matches := 0
		scratch := make([]functionalIndexBenchmarkRow, 0, scratchCap)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scratch = index.LookupInto(targetKey, scratch)
			matches += len(scratch)
		}
		b.StopTimer()
		b.ReportMetric(float64(matches)/float64(b.N), "matches/op")
		runtime.KeepAlive(index)
	})
}

func BenchmarkFunctionalIndexUpsert(b *testing.B) {
	const rowCount = 10000

	rows := make([]functionalIndexBenchmarkRow, rowCount)
	index, err := NewFunctionalIndex[functionalIndexBenchmarkRow, string](func(row functionalIndexBenchmarkRow) string {
		return row.Key
	}, rowCount)
	if err != nil {
		b.Fatal(err)
	}
	for i := range rows {
		rows[i] = functionalIndexBenchmarkRow{
			ID:    uint64(i + 1),
			Key:   fmt.Sprintf("key-%03d", i%1000),
			Value: i * 3,
		}
		if err := index.Upsert(rows[i].ID, rows[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("same-key", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			row := rows[i%len(rows)]
			if err := index.Upsert(row.ID, row); err != nil {
				b.Fatal(err)
			}
		}
		runtime.KeepAlive(index)
	})

	b.Run("key-change", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			row := functionalIndexBenchmarkRow{
				ID:    1,
				Key:   "functional-index-a",
				Value: i,
			}
			if i%2 != 0 {
				row.Key = "functional-index-b"
			}
			if err := index.Upsert(row.ID, row); err != nil {
				b.Fatal(err)
			}
		}
		runtime.KeepAlive(index)
	})
}
