package hatSql

import "testing"

func BenchmarkDebeziumChangefeedApply(b *testing.B) {
	const rowCount = 1024
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		b.Fatal(err)
	}
	initial := QuerySubscriptionDeltaBatch{Deltas: make([]QuerySubscriptionDelta, rowCount)}
	for index := range initial.Deltas {
		initial.Deltas[index] = QuerySubscriptionDelta{
			Row:  Row{"id": index, "status": "old", "value": index * 3},
			Diff: 1,
		}
	}
	if _, err := feed.Apply(initial); err != nil {
		b.Fatal(err)
	}
	oldToNew := make([]QuerySubscriptionDelta, 16)
	newToOld := make([]QuerySubscriptionDelta, 16)
	for index := 0; index < 8; index++ {
		oldRow := Row{"id": index, "status": "old", "value": index * 3}
		newRow := Row{"id": index, "status": "new", "value": index * 3}
		oldToNew[index*2] = QuerySubscriptionDelta{Row: oldRow, Diff: -1}
		oldToNew[index*2+1] = QuerySubscriptionDelta{Row: newRow, Diff: 1}
		newToOld[index*2] = QuerySubscriptionDelta{Row: newRow, Diff: -1}
		newToOld[index*2+1] = QuerySubscriptionDelta{Row: oldRow, Diff: 1}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch := oldToNew
		if index%2 == 1 {
			batch = newToOld
		}
		changes, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: batch})
		if err != nil {
			b.Fatal(err)
		}
		if len(changes) != 8 {
			b.Fatalf("changes = %d, want 8", len(changes))
		}
	}
}
