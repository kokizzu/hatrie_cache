package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkMZ037PerRecordDispatchBaseline(b *testing.B) {
	const workers = 4
	const partitions = 4
	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
		Partitions:   partitions,
		Capacity:     1024,
		MaxBatchSize: 64,
		Handler:      func(context.Context, int, []int) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := (index / workers) % partitions
		if err := batcher.Submit(partition, context.Background(), index); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMZ037WorkerLocalExchange(b *testing.B) {
	const workers = 4
	const partitions = 4
	exchange, err := NewWorkerLocalExchange(WorkerLocalExchangeOptions[int]{
		Workers:    workers,
		Partitions: partitions,
		Capacity:   1024,
		BatchSize:  64,
		Handler:    func(context.Context, int, []int) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		worker := index % workers
		partition := (index / workers) % partitions
		if err := exchange.Submit(worker, partition, context.Background(), index); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := exchange.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}
