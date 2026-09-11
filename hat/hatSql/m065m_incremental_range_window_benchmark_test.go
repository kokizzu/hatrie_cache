package hatSql

import "testing"

type m065mRangeBenchmarkRow struct {
	order int64
	value int64
}

var m065mRangeBenchmarkSink int64

func benchmarkM065mNaiveRange(rows []m065mRangeBenchmarkRow, preceding int64) int64 {
	var checksum int64
	for index, current := range rows {
		lower := current.order - preceding
		var sum int64
		for candidate := 0; candidate <= index; candidate++ {
			if rows[candidate].order >= lower {
				sum += rows[candidate].value
			}
		}
		checksum += sum
	}
	return checksum
}

func benchmarkM065mNaiveRangeMaterialized(rows []m065mRangeBenchmarkRow, preceding int64) int64 {
	updates := make([]DifferentialRow, 0, len(rows))
	var checksum int64
	for index, current := range rows {
		lower := current.order - preceding
		var sum int64
		for candidate := 0; candidate <= index; candidate++ {
			if rows[candidate].order >= lower {
				sum += rows[candidate].value
			}
		}
		updates = append(updates, DifferentialRow{
			Key:  "row-" + string(rune(index)),
			Diff: 1,
			Row: Row{
				"id":           "row-" + string(rune(index)),
				"order":        current.order,
				"value":        current.value,
				"window_value": sum,
			},
		})
		checksum += updates[len(updates)-1].Row["window_value"].(int64)
	}
	return checksum
}

func m065mRangeBenchmarkRows() []m065mRangeBenchmarkRow {
	rows := make([]m065mRangeBenchmarkRow, 4096)
	for index := range rows {
		rows[index] = m065mRangeBenchmarkRow{
			order: int64(index),
			value: int64(index%17 - 8),
		}
	}
	return rows
}

func BenchmarkM065mRangeNaiveBaseline(b *testing.B) {
	rows := m065mRangeBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m065mRangeBenchmarkSink = benchmarkM065mNaiveRange(rows, 64)
	}
}

func BenchmarkM065mRangeNaiveMaterialized(b *testing.B) {
	rows := m065mRangeBenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m065mRangeBenchmarkSink = benchmarkM065mNaiveRangeMaterialized(rows, 64)
	}
}
