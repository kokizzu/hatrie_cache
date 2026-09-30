package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM037KBatchRebuildGroupCount(b *testing.B) {
	updates := m037kBenchmarkUpdates(256)
	key := func(row SQLRow) string {
		return row["group"].(string)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		history := make([]DifferentialRow, 0, len(updates))
		for _, update := range updates {
			history = append(history, update)
			if _, err := GroupCountDifferentialRows(history, key); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkM037KStatefulGroupCount(b *testing.B) {
	updates := m037kBenchmarkUpdates(256)
	key := func(row SQLRow) string {
		return row["group"].(string)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		groupCount, err := NewIncrementalGroupCount(key)
		if err != nil {
			b.Fatal(err)
		}
		for _, update := range updates {
			if _, err := groupCount.Apply([]DifferentialRow{update}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkM037KStatefulGroupCountBatch(b *testing.B) {
	updates := m037kBenchmarkUpdates(256)
	key := func(row SQLRow) string {
		return row["group"].(string)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		groupCount, err := NewIncrementalGroupCount(key)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := groupCount.Apply(updates); err != nil {
			b.Fatal(err)
		}
	}
}

func m037kBenchmarkUpdates(size int) []DifferentialRow {
	updates := make([]DifferentialRow, size)
	for index := range updates {
		updates[index] = DifferentialRow{
			Key:  "row-" + strconv.Itoa(index),
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": "group-" + strconv.Itoa(index%32)},
		}
	}
	return updates
}
