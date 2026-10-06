package hatPipeline

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCHU01DurableAsyncBatcherSubmitAndFlush(b *testing.B) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: b.TempDir() + "/ids.hid"})
	if err != nil {
		b.Fatal(err)
	}
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[int]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[int]]{Capacity: 64, MaxBatchSize: 1, FlushInterval: time.Hour},
		Handler: func(_ context.Context, _ []AsyncInsert[int]) error { return nil },
	})
	if err != nil {
		_ = ledger.Close()
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		accepted, err := batcher.Submit(context.Background(), strconv.Itoa(index), index)
		if err != nil || !accepted {
			b.Fatalf("Submit() = %t/%v, want accepted", accepted, err)
		}
		if err := batcher.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	stats := ledger.Stats()
	if err := ledger.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(stats.FileBytes)/float64(b.N), "ledger_B/op")
}

func BenchmarkCHU01DurableAsyncBatcherDuplicateSubmit(b *testing.B) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: b.TempDir() + "/ids.hid"})
	if err != nil {
		b.Fatal(err)
	}
	acquired, err := ledger.Acquire("already-committed")
	if err != nil || !acquired {
		b.Fatalf("seed Acquire() = %t/%v", acquired, err)
	}
	if err := ledger.Commit("already-committed"); err != nil {
		b.Fatal(err)
	}
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[int]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[int]]{Capacity: 64, MaxBatchSize: 64, FlushInterval: time.Hour},
		Handler: func(_ context.Context, _ []AsyncInsert[int]) error { return nil },
	})
	if err != nil {
		_ = ledger.Close()
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		accepted, err := batcher.Submit(context.Background(), "already-committed", index)
		if err != nil || accepted {
			b.Fatalf("duplicate Submit() = %t/%v, want ignored", accepted, err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCHU01DurableAsyncBatcherBufferedSubmitAndFlush(b *testing.B) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{
		Path:       b.TempDir() + "/ids.hid",
		Durability: AsyncInsertLedgerDurabilityBuffered,
	})
	if err != nil {
		b.Fatal(err)
	}
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[int]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[int]]{Capacity: 64, MaxBatchSize: 1, FlushInterval: time.Hour},
		Handler: func(_ context.Context, _ []AsyncInsert[int]) error { return nil },
	})
	if err != nil {
		_ = ledger.Close()
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		accepted, err := batcher.Submit(context.Background(), strconv.Itoa(index), index)
		if err != nil || !accepted {
			b.Fatalf("Submit() = %t/%v, want accepted", accepted, err)
		}
		if err := batcher.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	stats := ledger.Stats()
	if err := ledger.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(stats.FileBytes)/float64(b.N), "ledger_B/op")
}

func BenchmarkCHU01DurableAsyncBatcherSubmit64AndFlush(b *testing.B) {
	const batchSize = 64
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: b.TempDir() + "/ids.hid"})
	if err != nil {
		b.Fatal(err)
	}
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[int]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[int]]{Capacity: batchSize * 2, MaxBatchSize: batchSize, FlushInterval: time.Hour},
		Handler: func(_ context.Context, _ []AsyncInsert[int]) error { return nil },
	})
	if err != nil {
		_ = ledger.Close()
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for value := 0; value < batchSize; value++ {
			id := strconv.Itoa(index*batchSize + value)
			accepted, err := batcher.Submit(context.Background(), id, value)
			if err != nil || !accepted {
				b.Fatalf("Submit() = %t/%v, want accepted", accepted, err)
			}
		}
		if err := batcher.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := batcher.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
	stats := ledger.Stats()
	if err := ledger.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(stats.FileBytes)/float64(b.N*batchSize), "ledger_B/item")
}
