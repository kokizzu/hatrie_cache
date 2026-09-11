package hatSql

import "testing"

func benchmarkM065oNaiveRangeDistinctMaterialized(rows []m065mRangeBenchmarkRow, preceding int64) int64 {
	updates := make([]DifferentialRow, 0, len(rows))
	var checksum int64
	for index, current := range rows {
		lower := current.order - preceding
		distinct := make(map[int64]struct{}, int(preceding)+1)
		for candidate := 0; candidate <= index; candidate++ {
			if rows[candidate].order < lower {
				continue
			}
			distinct[rows[candidate].value] = struct{}{}
		}
		value := int64(len(distinct))
		updates = append(updates, DifferentialRow{
			Key:  "row-" + string(rune(index)),
			Diff: 1,
			Row: Row{
				"id":           "row-" + string(rune(index)),
				"order":        current.order,
				"value":        current.value,
				"window_value": value,
			},
		})
		checksum += updates[len(updates)-1].Row["window_value"].(int64)
	}
	return checksum
}

func BenchmarkM065oRangeNaiveMaterialized(b *testing.B) {
	rows := m065mRangeBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m065mRangeBenchmarkSink = benchmarkM065oNaiveRangeDistinctMaterialized(rows, 64)
	}
}

func BenchmarkM065oRangeIncremental(b *testing.B) {
	benchmarkRows := m065mRangeBenchmarkRows()
	rows := make([]Row, len(benchmarkRows))
	for index, source := range benchmarkRows {
		rows[index] = Row{
			"id":    "row-" + string(rune(index)),
			"order": source.order,
			"value": source.value,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowCountDistinctInt64, 64, false))
		if err != nil {
			b.Fatal(err)
		}
		updates, err := window.Append(rows)
		if err != nil {
			b.Fatal(err)
		}
		var checksum int64
		for _, update := range updates {
			checksum += update.Row["window_value"].(int64)
		}
		m065mRangeBenchmarkSink = checksum
	}
}
