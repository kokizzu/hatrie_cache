package hatPipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkCHU01BaselineAsyncBatcherSubmit(b *testing.B) {
	var handled atomic.Uint64
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      8192,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, batch []int) error {
			handled.Add(uint64(len(batch)))
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := batcher.Submit(context.Background(), index); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	if handled.Load() != uint64(b.N) {
		b.Fatalf("handled = %d, want %d", handled.Load(), b.N)
	}
}

func BenchmarkCHU01BaselineAsyncBatcherSubmitBatch(b *testing.B) {
	var handled atomic.Uint64
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, batch []int) error {
			handled.Add(uint64(len(batch)))
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := make([]int, 64)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := batcher.SubmitBatch(context.Background(), values); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	want := uint64(b.N * len(values))
	if handled.Load() != want {
		b.Fatalf("handled = %d, want %d", handled.Load(), want)
	}
}

func BenchmarkCHU01BaselineAsyncBatcherSubmitAndFlush(b *testing.B) {
	var handled atomic.Uint64
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      64,
		MaxBatchSize:  1,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, batch []int) error {
			handled.Add(uint64(len(batch)))
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := batcher.Submit(context.Background(), index); err != nil {
			b.Fatal(err)
		}
		if err := batcher.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	if handled.Load() != uint64(b.N) {
		b.Fatalf("handled = %d, want %d", handled.Load(), b.N)
	}
}
