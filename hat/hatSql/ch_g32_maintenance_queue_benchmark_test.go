package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHG32ExistingIndexQueueEnqueueStatus(b *testing.B) {
	for index := 0; index < b.N; index++ {
		queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Capacity: 1, HistoryCapacity: 1})
		if err != nil {
			b.Fatal(err)
		}
		id := "index"
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{
			ID:   id,
			Name: "index",
			Run:  func(context.Context, SQLIndexRebuildProgressFunc) error { return nil },
		}); err != nil {
			b.Fatal(err)
		}
		if _, ok := queue.Status(id); !ok {
			b.Fatal("status missing")
		}
		if err := queue.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHG32MaintenanceQueueEnqueueStatus(b *testing.B) {
	for index := 0; index < b.N; index++ {
		queue, err := NewSQLMaintenanceQueue(SQLMaintenanceQueueOptions{Capacity: 1, HistoryCapacity: 1})
		if err != nil {
			b.Fatal(err)
		}
		id := "maintenance"
		if _, err := queue.Enqueue(SQLMaintenanceJobRequest{
			ID:   id,
			Name: "maintenance",
			Kind: SQLMaintenanceJobOptimize,
			Run:  func(context.Context, SQLMaintenanceProgressFunc) error { return nil },
		}); err != nil {
			b.Fatal(err)
		}
		if _, ok := queue.Status(id); !ok {
			b.Fatal("status missing")
		}
		if err := queue.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
