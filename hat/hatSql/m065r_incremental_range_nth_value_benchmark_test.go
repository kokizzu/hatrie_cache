package hatSql

import "testing"

func benchmarkM065rNaiveRangeNthMaterialized(rows []m065mRangeBenchmarkRow, preceding int64, position int) int64 {
	updates := make([]DifferentialRow, 0, len(rows))
	var checksum int64
	for index, current := range rows {
		lower := current.order - preceding
		seen := 0
		var value interface{}
		for candidate := 0; candidate <= index; candidate++ {
			if rows[candidate].order < lower {
				continue
			}
			seen++
			if seen == position {
				value = rows[candidate].value
				break
			}
		}
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
		if intValue, ok := value.(int64); ok {
			checksum += intValue
		}
	}
	return checksum
}

func BenchmarkM065rRangeNaiveMaterialized(b *testing.B) {
	rows := m065mRangeBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m065rRangeBenchmarkSink = benchmarkM065rNaiveRangeNthMaterialized(rows, 64, 3)
	}
}

func BenchmarkM065rRangeIncremental(b *testing.B) {
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
		window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(3, 64, false))
		if err != nil {
			b.Fatal(err)
		}
		updates, err := window.Append(rows)
		if err != nil {
			b.Fatal(err)
		}
		var checksum int64
		for _, update := range updates {
			if value, ok := update.Row["window_value"].(int64); ok {
				checksum += value
			}
		}
		m065rRangeBenchmarkSink = checksum
	}
}

var m065rRangeBenchmarkSink int64
