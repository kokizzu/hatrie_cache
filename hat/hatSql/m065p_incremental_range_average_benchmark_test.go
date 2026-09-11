package hatSql

import "testing"

func benchmarkM065pNaiveRangeAverageMaterialized(rows []m065mRangeBenchmarkRow, preceding int64) float64 {
	updates := make([]DifferentialRow, 0, len(rows))
	var checksum float64
	for index, current := range rows {
		lower := current.order - preceding
		var sum int64
		var count int64
		for candidate := 0; candidate <= index; candidate++ {
			if rows[candidate].order < lower {
				continue
			}
			sum += rows[candidate].value
			count++
		}
		value := float64(sum) / float64(count)
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
		checksum += updates[len(updates)-1].Row["window_value"].(float64)
	}
	return checksum
}

func BenchmarkM065pRangeNaiveMaterialized(b *testing.B) {
	rows := m065mRangeBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m065pRangeBenchmarkSink = benchmarkM065pNaiveRangeAverageMaterialized(rows, 64)
	}
}

func BenchmarkM065pRangeIncremental(b *testing.B) {
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
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowAvgInt64, 64, false))
		if err != nil {
			b.Fatal(err)
		}
		updates, err := window.Append(rows)
		if err != nil {
			b.Fatal(err)
		}
		var checksum float64
		for _, update := range updates {
			checksum += update.Row["window_value"].(float64)
		}
		m065pRangeBenchmarkSink = checksum
	}
}

var m065pRangeBenchmarkSink float64
