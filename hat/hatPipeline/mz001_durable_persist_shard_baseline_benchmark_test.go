package hatPipeline

import "testing"

type mz001BaselineRecord struct {
	key   string
	value []byte
}

func benchmarkMZ001SourceRows(count int) []mz001BaselineRecord {
	rows := make([]mz001BaselineRecord, count)
	for index := range rows {
		rows[index] = mz001BaselineRecord{
			key:   "customer-" + benchmarkMZ001Decimal(index),
			value: []byte("materialized-value-" + benchmarkMZ001Decimal(index)),
		}
	}
	return rows
}

func benchmarkMZ001Decimal(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}

func replayMZ001SourceRows(rows []mz001BaselineRecord) map[string][]byte {
	result := make(map[string][]byte, len(rows))
	for _, row := range rows {
		result[row.key] = append([]byte(nil), row.value...)
	}
	return result
}

var mz001BaselineResult map[string][]byte

func BenchmarkMZ001SourceReplayBaseline(b *testing.B) {
	rows := benchmarkMZ001SourceRows(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz001BaselineResult = replayMZ001SourceRows(rows)
	}
}
