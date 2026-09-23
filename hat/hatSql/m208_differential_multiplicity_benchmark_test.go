package hatSql

import "testing"

func m208DifferentialMultiplicityFixture() []QuerySubscriptionDelta {
	const rowCount = 512
	deltas := make([]QuerySubscriptionDelta, 0, rowCount*4)
	for index := 0; index < rowCount; index++ {
		row := Row{"id": index, "value": index % 17}
		deltas = append(deltas,
			QuerySubscriptionDelta{Row: row, Diff: 1},
			QuerySubscriptionDelta{Row: row, Diff: 1},
			QuerySubscriptionDelta{Row: row, Diff: -1},
			QuerySubscriptionDelta{Row: row, Diff: -1},
		)
	}
	return deltas
}

func benchmarkM208ManualConsolidate(deltas []QuerySubscriptionDelta) int64 {
	counts := make(map[string]int64, len(deltas))
	for _, delta := range deltas {
		counts[querySubscriptionRowKey(delta.Row)] += delta.Diff
	}
	var total int64
	for _, count := range counts {
		total += count
	}
	return total
}

var m208BenchmarkSink int64

func BenchmarkM208BaselineManualConsolidate(b *testing.B) {
	deltas := m208DifferentialMultiplicityFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		m208BenchmarkSink = benchmarkM208ManualConsolidate(deltas)
	}
}

func BenchmarkM208ConsolidateQuerySubscriptionDeltas(b *testing.B) {
	deltas := m208DifferentialMultiplicityFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		consolidated, err := ConsolidateQuerySubscriptionDeltas(deltas)
		if err != nil {
			b.Fatal(err)
		}
		m208BenchmarkSink = int64(len(consolidated))
	}
}
