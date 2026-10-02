package hatSql

import (
	"context"
	"testing"
)

func BenchmarkM35SQLSourceFrontierReadyAt(b *testing.B) {
	tracker := benchmarkM35ReadyTracker(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !tracker.ReadyAt(10) {
			b.Fatal("ReadyAt() returned false")
		}
	}
}

func BenchmarkM35SQLSourceFrontierWaitUntilReady(b *testing.B) {
	tracker := benchmarkM35ReadyTracker(b)
	ctx := context.Background()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := tracker.WaitUntil(ctx, 10); err != nil {
			b.Fatalf("WaitUntil() error = %v", err)
		}
	}
}

func benchmarkM35ReadyTracker(b *testing.B) *SQLSourceFrontierTracker {
	b.Helper()
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		b.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil {
		b.Fatalf("Observe() error = %v", err)
	}
	return tracker
}
