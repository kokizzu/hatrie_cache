package hatPipeline

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCHU23RegistrySnapshot(b *testing.B) {
	registry := newCHU23BenchmarkRegistry(b, 1)
	defer registry.Close(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.Snapshot()
	}
}

func BenchmarkCHU23RegistryFlush(b *testing.B) {
	registry := newCHU23BenchmarkRegistry(b, 1)
	defer registry.Close(context.Background())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Flush(ctx, "queue-0"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU23RegistryFlushAll16(b *testing.B) {
	registry := newCHU23BenchmarkRegistry(b, 16)
	defer registry.Close(context.Background())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.FlushAll(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func newCHU23BenchmarkRegistry(b *testing.B, count int) *AsyncBatcherRegistry {
	b.Helper()
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{MaxQueues: count})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < count; i++ {
		queue, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
			Capacity:      1,
			MaxBatchSize:  1,
			FlushInterval: time.Hour,
			Handler: func(context.Context, []int) error {
				return nil
			},
		})
		if err != nil {
			registry.Close(context.Background())
			b.Fatal(err)
		}
		if err := registry.Register("queue-"+itoaCHU23(i), queue); err != nil {
			queue.Close(context.Background())
			registry.Close(context.Background())
			b.Fatal(err)
		}
	}
	return registry
}

func itoaCHU23(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}
