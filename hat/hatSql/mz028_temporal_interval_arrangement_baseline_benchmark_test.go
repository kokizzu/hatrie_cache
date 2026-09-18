package hatSql

import (
	"testing"
	"time"
)

func BenchmarkMZ028BaselineTemporalTableOutOfOrderUpsert(b *testing.B) {
	const workload = 256
	rows := make([]Row, workload)
	timestamps := make([]time.Time, workload)
	for index := range rows {
		rows[index] = Row{"id": int64(index)}
		timestamps[index] = time.Unix(int64(index), 0)
	}
	var table *TemporalTable
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%workload == 0 {
			b.StopTimer()
			table = NewTemporalTable()
			b.StartTimer()
		}
		position := workload - 1 - index%workload
		table.Upsert("account", timestamps[position], rows[position])
	}
}

func BenchmarkMZ028BaselineTemporalTableAsOf(b *testing.B) {
	table := NewTemporalTable()
	for index := 0; index < 256; index++ {
		table.Upsert("account", time.Unix(int64(index), 0), Row{"id": int64(index)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		row, ok := table.AsOf("account", time.Unix(int64(index%256), 0))
		if !ok || row["id"] == nil {
			b.Fatal("missing temporal row")
		}
	}
}
