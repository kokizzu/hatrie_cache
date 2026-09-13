package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestC202PartitionedAsyncBatcherRoutesAndFlushesPerPartition(t *testing.T) {
	type receivedBatch struct {
		partition int
		values    []int
	}
	received := make(chan receivedBatch, 4)
	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
		Partitions:    2,
		Capacity:      4,
		MaxBatchSize:  2,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, partition int, batch []int) error {
			received <- receivedBatch{partition: partition, values: append([]int(nil), batch...)}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewPartitionedAsyncBatcher() error = %v", err)
	}

	for _, value := range []int{1, 2, 3} {
		if err := batcher.Submit(0, context.Background(), value); err != nil {
			t.Fatalf("Submit(0, %d) error = %v", value, err)
		}
	}
	for _, value := range []int{10, 11} {
		if err := batcher.Submit(1, context.Background(), value); err != nil {
			t.Fatalf("Submit(1, %d) error = %v", value, err)
		}
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	byPartition := map[int][]int{}
	for range 3 {
		batch := <-received
		byPartition[batch.partition] = append(byPartition[batch.partition], batch.values...)
	}
	if !reflect.DeepEqual(byPartition[0], []int{1, 2, 3}) {
		t.Fatalf("partition 0 values = %v, want [1 2 3]", byPartition[0])
	}
	if !reflect.DeepEqual(byPartition[1], []int{10, 11}) {
		t.Fatalf("partition 1 values = %v, want [10 11]", byPartition[1])
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stats := batcher.Stats()
	if stats.Submitted != 5 || stats.FlushedItems != 5 || stats.FlushedBatches != 3 || stats.Pending != 0 {
		t.Fatalf("Stats() = %#v, want five submitted/flushed items, three batches, no pending", stats)
	}
}

func TestC202PartitionedAsyncBatcherRunsPartitionsIndependently(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{})
	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{
		Partitions:    2,
		Capacity:      2,
		MaxBatchSize:  1,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, partition int, _ []int) error {
			started <- partition
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewPartitionedAsyncBatcher() error = %v", err)
	}
	if err := batcher.Submit(0, context.Background(), 1); err != nil {
		t.Fatalf("Submit(0) error = %v", err)
	}
	if err := batcher.Submit(1, context.Background(), 2); err != nil {
		t.Fatalf("Submit(1) error = %v", err)
	}
	seen := map[int]bool{}
	for range 2 {
		select {
		case partition := <-started:
			seen[partition] = true
		case <-time.After(time.Second):
			t.Fatal("both partition handlers did not start independently")
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("started partitions = %v, want both partitions", seen)
	}
	close(release)
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestC202PartitionedAsyncBatcherValidatesOptionsAndPartition(t *testing.T) {
	handler := func(context.Context, int, []int) error { return nil }
	for _, test := range []struct {
		name    string
		options PartitionedAsyncBatcherOptions[int]
		want    error
	}{
		{name: "handler", want: ErrPartitionedAsyncBatcherHandlerRequired},
		{name: "partitions", options: PartitionedAsyncBatcherOptions[int]{Partitions: 1, Handler: handler}, want: ErrPartitionedAsyncBatcherPartitionsInvalid},
		{name: "capacity", options: PartitionedAsyncBatcherOptions[int]{Partitions: 2, Capacity: 1, Handler: handler}, want: ErrPartitionedAsyncBatcherCapacityInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPartitionedAsyncBatcher(test.options); !errors.Is(err, test.want) {
				t.Fatalf("NewPartitionedAsyncBatcher() error = %v, want %v", err, test.want)
			}
		})
	}

	batcher, err := NewPartitionedAsyncBatcher(PartitionedAsyncBatcherOptions[int]{Partitions: 2, Handler: handler})
	if err != nil {
		t.Fatalf("NewPartitionedAsyncBatcher() error = %v", err)
	}
	if err := batcher.Submit(-1, context.Background(), 1); !errors.Is(err, ErrPartitionedAsyncBatcherPartitionInvalid) {
		t.Fatalf("negative partition error = %v", err)
	}
	if err := batcher.Submit(2, context.Background(), 1); !errors.Is(err, ErrPartitionedAsyncBatcherPartitionInvalid) {
		t.Fatalf("out-of-range partition error = %v", err)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
