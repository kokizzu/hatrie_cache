package hatSql

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkTR022IndexRebuildQueueWithoutVerification(b *testing.B) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Workers: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := "plain-" + strconv.Itoa(index)
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{
			ID:   id,
			Name: "orders",
			Run:  func(context.Context, SQLIndexRebuildProgressFunc) error { return nil },
		}); err != nil {
			b.Fatal(err)
		}
		if err := queue.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR022IndexRebuildQueueWithVerification(b *testing.B) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Workers: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := "verified-" + strconv.Itoa(index)
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{
			ID:     id,
			Name:   "orders",
			Run:    func(context.Context, SQLIndexRebuildProgressFunc) error { return nil },
			Verify: func(context.Context) error { return nil },
		}); err != nil {
			b.Fatal(err)
		}
		if err := queue.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
