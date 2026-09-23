package hatSql

import "testing"

var m213BenchmarkSink int

func m213BenchmarkBatch(marked bool) QuerySubscriptionDeltaBatch {
	rows := make([]Row, 128)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "value": index % 17}
	}
	batch := querySubscriptionInitialDeltaWithOrder(QuerySubscriptionSnapshot{
		Result: QueryResult{Rows: rows},
	}, false)
	if !marked {
		batch.consolidated = false
	}
	return batch
}

func BenchmarkM213GeneratedBatchBaseline(b *testing.B) {
	snapshot := QuerySubscriptionSnapshot{Result: QueryResult{Rows: make([]Row, 128)}}
	for index := range snapshot.Result.Rows {
		snapshot.Result.Rows[index] = Row{"id": int64(index), "value": index % 17}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := querySubscriptionInitialDeltaWithOrder(snapshot, false)
		m213BenchmarkSink = len(batch.Deltas)
	}
}

func BenchmarkM213GeneratedBatchWithBoundary(b *testing.B) {
	snapshot := QuerySubscriptionSnapshot{Result: QueryResult{Rows: make([]Row, 128)}}
	for index := range snapshot.Result.Rows {
		snapshot.Result.Rows[index] = Row{"id": int64(index), "value": index % 17}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := querySubscriptionInitialDeltaWithOrder(snapshot, false)
		consolidated, err := batch.Consolidate()
		if err != nil {
			b.Fatal(err)
		}
		m213BenchmarkSink = len(consolidated.Deltas)
	}
}

func BenchmarkM213UnmarkedBatchConsolidation(b *testing.B) {
	batch := m213BenchmarkBatch(false)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		consolidated, err := batch.Consolidate()
		if err != nil {
			b.Fatal(err)
		}
		m213BenchmarkSink = len(consolidated.Deltas)
	}
}
