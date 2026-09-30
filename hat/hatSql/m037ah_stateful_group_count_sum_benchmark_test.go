package hatSql

import (
	"fmt"
	"testing"
)

var m037AHCountSumBenchmarkSink []DifferentialRow

func m037AHBenchmarkGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func m037AHBenchmarkValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}

func m037AHBenchmarkUpdates() []DifferentialRow {
	updates := make([]DifferentialRow, 256)
	for index := range updates {
		updates[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%03d", index),
			Time: uint64(index + 1),
			Diff: 1,
			Row: Row{
				"group": fmt.Sprintf("g-%02d", index%32),
				"value": int64(index%17 + 1),
			},
		}
	}
	return updates
}

func BenchmarkM037AHCountSumFullHistoryRebuild(b *testing.B) {
	updates := m037AHBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for end := 1; end <= len(updates); end++ {
			result, err := GroupCountSumInt64DifferentialRows(updates[:end], m037AHBenchmarkGroupKey, m037AHBenchmarkValue)
			if err != nil {
				b.Fatal(err)
			}
			m037AHCountSumBenchmarkSink = result
		}
	}
}

func BenchmarkM037AHCountSumStatefulStreaming(b *testing.B) {
	updates := m037AHBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		groupCountSum, err := NewIncrementalGroupCountSumInt64(m037AHBenchmarkGroupKey, m037AHBenchmarkValue)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for index := range updates {
			result, err := groupCountSum.Apply(updates[index : index+1])
			if err != nil {
				b.Fatal(err)
			}
			m037AHCountSumBenchmarkSink = result
		}
		b.StopTimer()
	}
}

func BenchmarkM037AHCountSumStatefulBatch(b *testing.B) {
	updates := m037AHBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		groupCountSum, err := NewIncrementalGroupCountSumInt64(m037AHBenchmarkGroupKey, m037AHBenchmarkValue)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := groupCountSum.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		m037AHCountSumBenchmarkSink = result
		b.StopTimer()
	}
}
