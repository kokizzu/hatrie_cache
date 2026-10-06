package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkCHU23BaselineBatcherStats(b *testing.B) {
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:    1,
		MaxBatchSize: 1,
		Handler: func(context.Context, []int) error {
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer batcher.Close(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batcher.Stats()
	}
}

func BenchmarkCHU23BaselineBatcherFlush(b *testing.B) {
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:    1,
		MaxBatchSize: 1,
		Handler: func(context.Context, []int) error {
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer batcher.Close(context.Background())

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := batcher.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
