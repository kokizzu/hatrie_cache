package hatSql

import (
	"fmt"
	"testing"
)

var m037LSumBenchmarkSink []DifferentialRow

func m037LBenchmarkGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func m037LBenchmarkValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}

func m037LBenchmarkUpdates() []DifferentialRow {
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

func BenchmarkM037LSumFullHistoryRebuild(b *testing.B) {
	updates := m037LBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for end := 1; end <= len(updates); end++ {
			result, err := GroupSumInt64DifferentialRows(updates[:end], m037LBenchmarkGroupKey, m037LBenchmarkValue)
			if err != nil {
				b.Fatal(err)
			}
			m037LSumBenchmarkSink = result
		}
	}
}

func BenchmarkM037LSumStatefulStreaming(b *testing.B) {
	updates := m037LBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		groupSum, err := NewIncrementalGroupSumInt64(m037LBenchmarkGroupKey, m037LBenchmarkValue)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for index := range updates {
			result, err := groupSum.Apply(updates[index : index+1])
			if err != nil {
				b.Fatal(err)
			}
			m037LSumBenchmarkSink = result
		}
		b.StopTimer()
	}
}

func BenchmarkM037LSumStatefulBatch(b *testing.B) {
	updates := m037LBenchmarkUpdates()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		groupSum, err := NewIncrementalGroupSumInt64(m037LBenchmarkGroupKey, m037LBenchmarkValue)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := groupSum.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		m037LSumBenchmarkSink = result
		b.StopTimer()
	}
}
