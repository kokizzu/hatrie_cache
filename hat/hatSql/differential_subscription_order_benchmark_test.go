package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkQuerySubscriptionDeltaBatchOrder(b *testing.B) {
	const rowCount = 128
	rows := make([]Row, rowCount)
	for index := range rows {
		rows[index] = Row{
			"id":    strconv.Itoa(rowCount - index),
			"value": int64(index),
		}
	}
	snapshot := QuerySubscriptionSnapshot{
		ID:       1,
		Revision: 2,
		Frontier: 3,
		Result: QueryResult{
			Columns: []string{"id", "value"},
			Rows:    rows,
		},
	}

	b.Run("resolver_order", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			batch := querySubscriptionDeltaBatch(snapshot, QueryResult{}, false)
			if len(batch.Deltas) != rowCount {
				b.Fatalf("delta count = %d, want %d", len(batch.Deltas), rowCount)
			}
		}
	})
	b.Run("deterministic_order", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			batch := querySubscriptionDeltaBatchWithOrder(snapshot, QueryResult{}, false, true)
			if len(batch.Deltas) != rowCount {
				b.Fatalf("delta count = %d, want %d", len(batch.Deltas), rowCount)
			}
		}
	})
}
