package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrPartitionedAsyncBatcherNil indicates that a method was called on a
	// nil partitioned batcher.
	ErrPartitionedAsyncBatcherNil = errors.New("hatPipeline: partitioned async batcher is nil")
	// ErrPartitionedAsyncBatcherHandlerRequired indicates that no partitioned
	// batch handler was supplied.
	ErrPartitionedAsyncBatcherHandlerRequired = errors.New("hatPipeline: partitioned async batch handler is required")
	// ErrPartitionedAsyncBatcherPartitionsInvalid indicates an unsupported
	// number of independent partition workers.
	ErrPartitionedAsyncBatcherPartitionsInvalid = errors.New("hatPipeline: partitioned async batcher partition count is invalid")
	// ErrPartitionedAsyncBatcherCapacityInvalid indicates that total queue
	// capacity cannot provide at least one slot to every partition.
	ErrPartitionedAsyncBatcherCapacityInvalid = errors.New("hatPipeline: partitioned async batcher capacity is invalid")
	// ErrPartitionedAsyncBatcherPartitionInvalid indicates an out-of-range
	// partition index.
	ErrPartitionedAsyncBatcherPartitionInvalid = errors.New("hatPipeline: partitioned async batcher partition index is invalid")
)

const (
	// MaxPartitionedAsyncBatcherPartitions bounds worker and queue creation.
	MaxPartitionedAsyncBatcherPartitions = 256
)

// PartitionedAsyncBatcherOptions configures independent bounded batchers.
// Capacity is the total queued-value budget and is distributed as evenly as
// possible across partitions. The caller chooses the partition for each
// value; no hash routing or automatic sharding is performed.
type PartitionedAsyncBatcherOptions[T any] struct {
	Partitions    int
	Capacity      int
	MaxBatchSize  int
	FlushInterval time.Duration
	Context       context.Context
	Handler       func(context.Context, int, []T) error
}

// PartitionedAsyncBatcherStats is a point-in-time aggregate plus one snapshot
// for each partition. The Partitions slice is a new slice on every call.
type PartitionedAsyncBatcherStats struct {
	Submitted      uint64
	FlushedBatches uint64
	FlushedItems   uint64
	HandlerErrors  uint64
	Rejected       uint64
	Pending        int
	Partitions     []AsyncBatcherStats
}

// PartitionedAsyncBatcher keeps one bounded queue and worker per partition.
// Handlers for different partitions may run concurrently, while each handler
// remains serial and ordered within its own partition.
type PartitionedAsyncBatcher[T any] struct {
	batchers []*AsyncBatcher[T]
}

// NewPartitionedAsyncBatcher creates an explicitly partitioned batcher. At
// least two and at most MaxPartitionedAsyncBatcherPartitions workers are
// required because one partition has no affinity benefit. A zero Capacity,
// MaxBatchSize, or FlushInterval uses the corresponding AsyncBatcher default.
func NewPartitionedAsyncBatcher[T any](options PartitionedAsyncBatcherOptions[T]) (*PartitionedAsyncBatcher[T], error) {
	if options.Handler == nil {
		return nil, ErrPartitionedAsyncBatcherHandlerRequired
	}
	if options.Partitions < 2 || options.Partitions > MaxPartitionedAsyncBatcherPartitions {
		return nil, ErrPartitionedAsyncBatcherPartitionsInvalid
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultAsyncBatcherCapacity
	}
	if options.Capacity < options.Partitions || options.Capacity > MaxAsyncBatcherCapacity {
		return nil, ErrPartitionedAsyncBatcherCapacityInvalid
	}
	if options.MaxBatchSize == 0 {
		options.MaxBatchSize = DefaultAsyncBatcherMaxBatchSize
	}
	if options.MaxBatchSize < 1 || options.MaxBatchSize > MaxAsyncBatcherBatchSize {
		return nil, ErrAsyncBatcherMaxBatchSizeInvalid
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = DefaultAsyncBatcherFlushInterval
	}
	if options.FlushInterval <= 0 {
		return nil, ErrAsyncBatcherFlushIntervalInvalid
	}
	if options.Context == nil {
		options.Context = context.Background()
	}

	batchers := make([]*AsyncBatcher[T], options.Partitions)
	baseCapacity := options.Capacity / options.Partitions
	extraCapacity := options.Capacity % options.Partitions
	for partition := range batchers {
		capacity := baseCapacity
		if partition < extraCapacity {
			capacity++
		}
		partition := partition
		current, err := NewAsyncBatcher(AsyncBatcherOptions[T]{
			Capacity:      capacity,
			MaxBatchSize:  options.MaxBatchSize,
			FlushInterval: options.FlushInterval,
			Context:       options.Context,
			Handler: func(ctx context.Context, batch []T) error {
				return options.Handler(ctx, partition, batch)
			},
		})
		if err != nil {
			for _, created := range batchers[:partition] {
				_ = created.Close(context.Background())
			}
			return nil, err
		}
		batchers[partition] = current
	}
	return &PartitionedAsyncBatcher[T]{batchers: batchers}, nil
}

// PartitionCount returns the number of independent partitions. A nil
// receiver returns zero.
func (batcher *PartitionedAsyncBatcher[T]) PartitionCount() int {
	if batcher == nil {
		return 0
	}
	return len(batcher.batchers)
}

// Submit queues one value on the caller-selected partition. Values submitted
// to different partitions may be handled concurrently and have no global
// ordering guarantee.
func (batcher *PartitionedAsyncBatcher[T]) Submit(partition int, ctx context.Context, value T) error {
	if batcher == nil {
		return ErrPartitionedAsyncBatcherNil
	}
	if partition < 0 || partition >= len(batcher.batchers) {
		return ErrPartitionedAsyncBatcherPartitionInvalid
	}
	return batcher.batchers[partition].Submit(ctx, value)
}

// Flush waits for all values accepted before each partition's marker and
// returns handler errors joined in ascending partition order.
func (batcher *PartitionedAsyncBatcher[T]) Flush(ctx context.Context) error {
	if batcher == nil {
		return ErrPartitionedAsyncBatcherNil
	}
	return batcher.runAll(func(current *AsyncBatcher[T]) error {
		return current.Flush(ctx)
	})
}

// Close drains every partition and waits for all workers. Handler errors are
// joined in ascending partition order.
func (batcher *PartitionedAsyncBatcher[T]) Close(ctx context.Context) error {
	if batcher == nil {
		return ErrPartitionedAsyncBatcherNil
	}
	return batcher.runAll(func(current *AsyncBatcher[T]) error {
		return current.Close(ctx)
	})
}

// Stats returns aggregate counters and independent per-partition snapshots.
func (batcher *PartitionedAsyncBatcher[T]) Stats() PartitionedAsyncBatcherStats {
	if batcher == nil {
		return PartitionedAsyncBatcherStats{}
	}
	stats := PartitionedAsyncBatcherStats{Partitions: make([]AsyncBatcherStats, len(batcher.batchers))}
	for partition, current := range batcher.batchers {
		partStats := current.Stats()
		stats.Partitions[partition] = partStats
		stats.Submitted += partStats.Submitted
		stats.FlushedBatches += partStats.FlushedBatches
		stats.FlushedItems += partStats.FlushedItems
		stats.HandlerErrors += partStats.HandlerErrors
		stats.Rejected += partStats.Rejected
		stats.Pending += partStats.Pending
	}
	return stats
}

func (batcher *PartitionedAsyncBatcher[T]) runAll(run func(*AsyncBatcher[T]) error) error {
	errorsByPartition := make([]error, len(batcher.batchers))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(batcher.batchers))
	for partition, current := range batcher.batchers {
		go func(partition int, current *AsyncBatcher[T]) {
			defer waitGroup.Done()
			errorsByPartition[partition] = run(current)
		}(partition, current)
	}
	waitGroup.Wait()
	return errors.Join(errorsByPartition...)
}
