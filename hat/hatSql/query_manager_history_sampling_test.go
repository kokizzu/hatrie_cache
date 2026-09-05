package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func TestSQLQueryManagerHistorySamplingIsDeterministicAndBounded(t *testing.T) {
	manager := NewSQLQueryManagerWithOptions(SQLQueryManagerOptions{
		HistoryCapacity:    2,
		HistorySampleEvery: 2,
	})
	for index := 1; index <= 5; index++ {
		_, err := manager.Execute(
			context.Background(),
			"FROM VALUES (1) AS item(value) SELECT value",
			nil,
			nil,
			SQLQueryOptions{QueryID: fmt.Sprintf("q%d", index)},
		)
		if err != nil {
			t.Fatalf("query %d error = %v", index, err)
		}
	}
	history := manager.History()
	if len(history) != 2 || history[0].QueryID != "q3" || history[1].QueryID != "q5" {
		t.Fatalf("sampled history = %#v, want q3 and q5", history)
	}
}

func BenchmarkSQLQueryManagerHistoryAppendSampling(b *testing.B) {
	for _, sampleEvery := range []int{0, 100} {
		b.Run(fmt.Sprintf("every-%d", sampleEvery), func(b *testing.B) {
			manager := newSQLQueryManager(256, sampleEvery)
			status := SQLQueryStatus{QueryID: "query"}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				manager.mu.Lock()
				manager.appendHistoryLocked(status)
				manager.mu.Unlock()
			}
		})
	}
}
