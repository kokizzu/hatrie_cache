package hatPipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkC202GlobalAsyncBatcher(b *testing.B) {
	var checksum uint64
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, batch []int) error {
			var sum uint64
			for _, value := range batch {
				sum += c202BatchWork(value)
			}
			checksum = sum
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		value := 0
		for pb.Next() {
			if err := batcher.Submit(context.Background(), value); err != nil {
				b.Fatal(err)
			}
			value++
		}
	})
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(checksum), "checksum")
}

func BenchmarkC202GlobalAsyncBatcherNoWork(b *testing.B) {
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler:       func(context.Context, []int) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		value := 0
		for pb.Next() {
			if err := batcher.Submit(context.Background(), value); err != nil {
				b.Fatal(err)
			}
			value++
		}
	})
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkC202PartitionedAsyncBatcher(b *testing.B) {
	const partitions = 4
	var checksums [partitions]uint64
	var nextPartition atomic.Uint32
	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
		Partitions:    partitions,
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, partition int, batch []int) error {
			var sum uint64
			for _, value := range batch {
				sum += c202BatchWork(value)
			}
			checksums[partition] = sum
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		partition := int(nextPartition.Add(1)-1) % partitions
		value := 0
		for pb.Next() {
			if err := batcher.Submit(partition, context.Background(), value); err != nil {
				b.Fatal(err)
			}
			value++
		}
	})
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	var checksum uint64
	for _, value := range checksums {
		checksum ^= value
	}
	b.ReportMetric(float64(checksum), "checksum")
}

func BenchmarkC202PartitionedAsyncBatcherNoWork(b *testing.B) {
	const partitions = 4
	var nextPartition atomic.Uint32
	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
		Partitions:    partitions,
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler:       func(context.Context, int, []int) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		partition := int(nextPartition.Add(1)-1) % partitions
		value := 0
		for pb.Next() {
			if err := batcher.Submit(partition, context.Background(), value); err != nil {
				b.Fatal(err)
			}
			value++
		}
	})
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkC202PartitionedAsyncBatcherSetup(b *testing.B) {
	const partitions = 4
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
			Partitions:    partitions,
			Capacity:      1024,
			MaxBatchSize:  64,
			FlushInterval: time.Hour,
			Handler:       func(context.Context, int, []int) error { return nil },
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := batcher.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(partitions, "partitions")
}

func BenchmarkC202GlobalAsyncBatcherSetup(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
			Capacity:      1024,
			MaxBatchSize:  64,
			FlushInterval: time.Hour,
			Handler:       func(context.Context, []int) error { return nil },
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := batcher.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func c202BatchWork(value int) uint64 {
	state := uint64(value) + 0x9e3779b97f4a7c15
	for index := 0; index < 32; index++ {
		state ^= state >> 30
		state *= 0xbf58476d1ce4e5b9
		state ^= state >> 27
		state *= 0x94d049bb133111eb
		state ^= state >> 31
	}
	return state
}
