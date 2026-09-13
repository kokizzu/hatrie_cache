package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestMZ037WorkerLocalExchangeBatchesAndRoutesValues(t *testing.T) {
	type receivedBatch struct {
		partition int
		values    []int
	}
	var mu sync.Mutex
	received := make([]receivedBatch, 0, 4)
	exchange, err := NewWorkerLocalExchange(WorkerLocalExchangeOptions[int]{
		Workers:    2,
		Partitions: 2,
		Capacity:   16,
		BatchSize:  3,
		Handler: func(_ context.Context, partition int, values []int) error {
			mu.Lock()
			received = append(received, receivedBatch{partition: partition, values: append([]int(nil), values...)})
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorkerLocalExchange() error = %v", err)
	}
	for _, value := range []int{1, 2, 3, 4} {
		if err := exchange.Submit(0, 0, context.Background(), value); err != nil {
			t.Fatalf("Submit(worker 0, partition 0, %d) error = %v", value, err)
		}
	}
	if err := exchange.Submit(0, 1, context.Background(), 10); err != nil {
		t.Fatalf("Submit(worker 0, partition 1) error = %v", err)
	}
	for _, value := range []int{20, 21} {
		if err := exchange.Submit(1, 0, context.Background(), value); err != nil {
			t.Fatalf("Submit(worker 1, partition 0, %d) error = %v", value, err)
		}
	}
	if err := exchange.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := exchange.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	byPartition := map[int][]int{}
	for _, batch := range received {
		byPartition[batch.partition] = append(byPartition[batch.partition], batch.values...)
	}
	if !reflect.DeepEqual(byPartition[0], []int{1, 2, 3, 4, 20, 21}) {
		t.Fatalf("partition 0 values = %v, want [1 2 3 4 20 21]", byPartition[0])
	}
	if !reflect.DeepEqual(byPartition[1], []int{10}) {
		t.Fatalf("partition 1 values = %v, want [10]", byPartition[1])
	}
	stats := exchange.Stats()
	if stats.Submitted != 7 || stats.FlushedItems != 7 || stats.FlushedBatches != 4 || stats.Buffered != 0 {
		t.Fatalf("Stats() = %#v, want seven submitted/flushed items, four batches, no buffered values", stats)
	}
}

func TestMZ037WorkerLocalExchangeValidatesLifecycleAndRouting(t *testing.T) {
	handler := func(context.Context, int, []int) error { return nil }
	for _, test := range []struct {
		name    string
		options WorkerLocalExchangeOptions[int]
		want    error
	}{
		{name: "handler", want: ErrWorkerLocalExchangeHandlerRequired},
		{name: "workers", options: WorkerLocalExchangeOptions[int]{Workers: -1, Handler: handler}, want: ErrWorkerLocalExchangeWorkersInvalid},
		{name: "partitions", options: WorkerLocalExchangeOptions[int]{Partitions: -1, Handler: handler}, want: ErrWorkerLocalExchangePartitionsInvalid},
		{name: "capacity", options: WorkerLocalExchangeOptions[int]{Workers: 2, Partitions: 2, Capacity: 1, Handler: handler}, want: ErrWorkerLocalExchangeCapacityInvalid},
		{name: "batch size", options: WorkerLocalExchangeOptions[int]{Workers: 2, Partitions: 2, BatchSize: -1, Handler: handler}, want: ErrWorkerLocalExchangeBatchSizeInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewWorkerLocalExchange(test.options); !errors.Is(err, test.want) {
				t.Fatalf("NewWorkerLocalExchange() error = %v, want %v", err, test.want)
			}
		})
	}

	exchange, err := NewWorkerLocalExchange(WorkerLocalExchangeOptions[int]{Workers: 2, Partitions: 2, Handler: handler})
	if err != nil {
		t.Fatalf("NewWorkerLocalExchange() error = %v", err)
	}
	if err := exchange.Submit(-1, 0, context.Background(), 1); !errors.Is(err, ErrWorkerLocalExchangeWorkerInvalid) {
		t.Fatalf("negative worker error = %v, want %v", err, ErrWorkerLocalExchangeWorkerInvalid)
	}
	if err := exchange.Submit(0, 2, context.Background(), 1); !errors.Is(err, ErrWorkerLocalExchangePartitionInvalid) {
		t.Fatalf("out-of-range partition error = %v, want %v", err, ErrWorkerLocalExchangePartitionInvalid)
	}
	if err := exchange.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := exchange.Submit(0, 0, context.Background(), 1); !errors.Is(err, ErrWorkerLocalExchangeClosed) {
		t.Fatalf("submit after close error = %v, want %v", err, ErrWorkerLocalExchangeClosed)
	}
}

func TestMZ037AsyncBatcherSubmitBatchPreservesOrderAndBounds(t *testing.T) {
	received := make(chan []int, 3)
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      4,
		MaxBatchSize:  3,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, values []int) error {
			received <- append([]int(nil), values...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}
	if err := batcher.Submit(context.Background(), 1); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := batcher.SubmitBatch(context.Background(), []int{2, 3}); err != nil {
		t.Fatalf("SubmitBatch() error = %v", err)
	}
	if err := batcher.Submit(context.Background(), 4); err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	if err := batcher.SubmitBatch(context.Background(), []int{5, 6, 7, 8}); !errors.Is(err, ErrAsyncBatcherBatchTooLarge) {
		t.Fatalf("oversized SubmitBatch() error = %v, want %v", err, ErrAsyncBatcherBatchTooLarge)
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	for index, expected := range [][]int{{1}, {2, 3}, {4}} {
		select {
		case got := <-received:
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("handler batch %d = %v, want %v", index, got, expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("handler batch %d was not received", index)
		}
	}
	stats := batcher.Stats()
	if stats.Submitted != 4 || stats.FlushedItems != 4 || stats.FlushedBatches != 3 || stats.Rejected != 4 || stats.Pending != 0 {
		t.Fatalf("Stats() = %#v, want four submitted/flushed items, four rejected items, three batches, no pending", stats)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestMZ037WorkerLocalExchangePropagatesHandlerErrors(t *testing.T) {
	wantErr := errors.New("handler failed")
	exchange, err := NewWorkerLocalExchange(WorkerLocalExchangeOptions[int]{
		BatchSize: 2,
		Handler: func(_ context.Context, _ int, values []int) error {
			if !reflect.DeepEqual(values, []int{1, 2}) {
				t.Fatalf("handler values = %v, want [1 2]", values)
			}
			return wantErr
		},
	})
	if err != nil {
		t.Fatalf("NewWorkerLocalExchange() error = %v", err)
	}
	if err := exchange.Submit(0, 0, context.Background(), 1); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	if err := exchange.Submit(0, 0, context.Background(), 2); err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	if err := exchange.Flush(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Flush() error = %v, want %v", err, wantErr)
	}
	stats := exchange.Stats()
	if stats.Submitted != 2 || stats.FlushedItems != 2 || stats.HandlerErrors != 1 || stats.Buffered != 0 || stats.Pending != 0 {
		t.Fatalf("Stats() = %#v, want two submitted/flushed items, one handler error, no pending", stats)
	}
	if err := exchange.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
