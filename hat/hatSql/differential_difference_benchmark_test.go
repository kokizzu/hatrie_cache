package hatSql

import (
	"fmt"
	"testing"
)

var differentialDifferenceBenchmarkSink int

func benchmarkDifferentialDifferenceFixtures() ([]DifferentialRow, []DifferentialRow) {
	left := make([]DifferentialRow, 4096)
	right := make([]DifferentialRow, 4096)
	for index := range left {
		key := fmt.Sprintf("key-%04d", index%2048)
		time := uint64(index % 64)
		row := Row{"value": int64(index % 2048)}
		left[index] = DifferentialRow{Key: key, Time: time, Diff: 1, Row: row}
		right[index] = DifferentialRow{Key: key, Time: time, Diff: 1, Row: row}
	}
	return left, right
}

func benchmarkDifferentialDifferenceBaseline(left, right []DifferentialRow) ([]DifferentialRow, error) {
	combined := make([]DifferentialRow, 0, len(left)+len(right))
	for _, update := range left {
		update.Row = cloneDifferentialRow(update.Row)
		combined = append(combined, update)
	}
	for _, update := range right {
		if update.Diff == -1<<63 {
			return nil, fmt.Errorf("differential difference diff overflow")
		}
		update.Diff = -update.Diff
		update.Row = cloneDifferentialRow(update.Row)
		combined = append(combined, update)
	}
	return ConsolidateDifferentialRows(combined)
}

func BenchmarkExceptDifferentialRows(b *testing.B) {
	left, right := benchmarkDifferentialDifferenceFixtures()
	b.Run("BaselineComposition", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			result, err := benchmarkDifferentialDifferenceBaseline(left, right)
			if err != nil {
				b.Fatal(err)
			}
			differentialDifferenceBenchmarkSink += len(result)
		}
	})
	b.Run("Optimized", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			result, err := ExceptDifferentialRows(left, right)
			if err != nil {
				b.Fatal(err)
			}
			differentialDifferenceBenchmarkSink += len(result)
		}
	})
}
