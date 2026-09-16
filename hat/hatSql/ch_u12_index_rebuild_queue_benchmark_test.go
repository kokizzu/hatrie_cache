package hatSql

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkSQLIndexRebuildQueueEnqueue(b *testing.B) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Capacity: 100_000, Workers: 1, HistoryCapacity: 100_000})
	if err != nil {
		b.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	run := func(context.Context, SQLIndexRebuildProgressFunc) error {
		atomic.AddUint64(&sqlIndexRebuildQueueBenchmarkSink, 1)
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	const batchSize = 1024
	for index := 0; index < b.N; index++ {
		if index > 0 && index%batchSize == 0 {
			b.StopTimer()
			if err := queue.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "benchmark-" + strconv.Itoa(index), Name: "benchmark", Run: run}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := queue.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
}
