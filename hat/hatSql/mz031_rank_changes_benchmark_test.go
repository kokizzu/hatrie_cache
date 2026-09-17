package hatSql

import (
	"sort"
	"testing"
)

type mz031RankBenchmarkRow struct {
	key   string
	score int64
}

type mz031RankBenchmarkOutput struct {
	key  string
	rank int
}

func BenchmarkMZ031RankedTopKRebuildBaseline(b *testing.B) {
	const (
		rows = 10000
		k    = 20
	)
	values := make([]mz031RankBenchmarkRow, rows)
	for index := range values {
		values[index] = mz031RankBenchmarkRow{key: "row-" + string(rune(index)), score: int64(index)}
	}
	ordered := make([]mz031RankBenchmarkRow, rows)
	output := make([]mz031RankBenchmarkOutput, k)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		values[index].score = int64((iteration*104729 + 23) % (rows * 4))
		copy(ordered, values)
		sort.Slice(ordered, func(left, right int) bool {
			if ordered[left].score != ordered[right].score {
				return ordered[left].score > ordered[right].score
			}
			return ordered[left].key < ordered[right].key
		})
		for index := 0; index < k; index++ {
			output[index] = mz031RankBenchmarkOutput{key: ordered[index].key, rank: index + 1}
			checksum += int64(output[index].rank) + ordered[index].score
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}
