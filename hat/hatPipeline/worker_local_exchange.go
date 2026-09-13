package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrWorkerLocalExchangeNil indicates that a method was called on a nil
	// worker-local exchange.
	ErrWorkerLocalExchangeNil = errors.New("hatPipeline: worker-local exchange is nil")
	// ErrWorkerLocalExchangeHandlerRequired indicates that no partition handler
	// was supplied.
	ErrWorkerLocalExchangeHandlerRequired = errors.New("hatPipeline: worker-local exchange handler is required")
	// ErrWorkerLocalExchangeWorkersInvalid indicates an unsupported worker count.
	ErrWorkerLocalExchangeWorkersInvalid = errors.New("hatPipeline: worker-local exchange worker count is invalid")
	// ErrWorkerLocalExchangePartitionsInvalid indicates an unsupported partition
	// count.
	ErrWorkerLocalExchangePartitionsInvalid = errors.New("hatPipeline: worker-local exchange partition count is invalid")
	// ErrWorkerLocalExchangeCapacityInvalid indicates that the queue cannot hold
	// one complete batch for every partition.
	ErrWorkerLocalExchangeCapacityInvalid = errors.New("hatPipeline: worker-local exchange capacity is invalid")
	// ErrWorkerLocalExchangeBatchSizeInvalid indicates an unsupported local batch
	// size.
	ErrWorkerLocalExchangeBatchSizeInvalid = errors.New("hatPipeline: worker-local exchange batch size is invalid")
	// ErrWorkerLocalExchangeWorkerInvalid indicates an out-of-range worker index.
	ErrWorkerLocalExchangeWorkerInvalid = errors.New("hatPipeline: worker-local exchange worker index is invalid")
	// ErrWorkerLocalExchangePartitionInvalid indicates an out-of-range partition
	// index.
	ErrWorkerLocalExchangePartitionInvalid = errors.New("hatPipeline: worker-local exchange partition index is invalid")
	// ErrWorkerLocalExchangeClosed indicates that the exchange no longer accepts
	// values.
	ErrWorkerLocalExchangeClosed = errors.New("hatPipeline: worker-local exchange is closed")
)

const (
	// MaxWorkerLocalExchangeWorkers bounds local buffer creation.
	MaxWorkerLocalExchangeWorkers = 256
	// MaxWorkerLocalExchangePartitions bounds partition worker and queue
	// creation.
	MaxWorkerLocalExchangePartitions = 256
)

// WorkerLocalExchangeOptions configures an explicitly enabled worker-local
// exchange. Each worker owns one producer slot and appends to a private buffer
// for each partition. A full buffer is transferred as one batch to that
// partition's asynchronous worker.
//
// Workers and Partitions default to one. Capacity, BatchSize, and
// FlushInterval use the corresponding AsyncBatcher defaults when zero. Capacity
// is distributed across partitions and must hold at least one complete batch
// per partition. Handler calls for each partition are serial and receive a
// caller-owned partition index.
type WorkerLocalExchangeOptions[T any] struct {
	Workers       int
	Partitions    int
	Capacity      int
	BatchSize     int
	FlushInterval time.Duration
	Context       context.Context
	Handler       func(context.Context, int, []T) error
}

// WorkerLocalExchangeStats is a point-in-time exchange snapshot. Buffered is
// the number of values still held in private worker buffers. Pending includes
// those values and values queued or currently handled by partition workers.
type WorkerLocalExchangeStats struct {
	Submitted      uint64
	FlushedBatches uint64
	FlushedItems   uint64
	HandlerErrors  uint64
	Rejected       uint64
	Buffered       int
	Pending        int
	Partitions     []AsyncBatcherStats
}

// WorkerLocalExchange reduces coordination on a hot producer path by keeping a
// bounded buffer per worker and partition. Submit is safe concurrently for
// different worker indexes, provided one producer owns each worker index.
// Submit, Flush, and Close must not run concurrently, and a worker index must
// not be submitted by more than one producer at a time.
type WorkerLocalExchange[T any] struct {
	batchers   []*AsyncBatcher[T]
	buffers    [][]T
	bufferPool *sync.Pool
	workers    int
	partitions int
	batchSize  int

	closed atomic.Bool

	closeMu sync.Mutex

	submitted atomic.Uint64
	rejected  atomic.Uint64
	buffered  atomic.Int64
}

// NewWorkerLocalExchange creates a running worker-local exchange. The feature
// is opt-in: constructing this type does not alter AsyncBatcher or
// PartitionedAsyncBatcher defaults.
func NewWorkerLocalExchange[T any](options WorkerLocalExchangeOptions[T]) (*WorkerLocalExchange[T], error) {
	if options.Handler == nil {
		return nil, ErrWorkerLocalExchangeHandlerRequired
	}
	if options.Workers == 0 {
		options.Workers = 1
	}
	if options.Workers < 1 || options.Workers > MaxWorkerLocalExchangeWorkers {
		return nil, ErrWorkerLocalExchangeWorkersInvalid
	}
	if options.Partitions == 0 {
		options.Partitions = 1
	}
	if options.Partitions < 1 || options.Partitions > MaxWorkerLocalExchangePartitions {
		return nil, ErrWorkerLocalExchangePartitionsInvalid
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultAsyncBatcherCapacity
	}
	if options.Capacity < 1 || options.Capacity > MaxAsyncBatcherCapacity {
		return nil, ErrWorkerLocalExchangeCapacityInvalid
	}
	if options.BatchSize == 0 {
		options.BatchSize = DefaultAsyncBatcherMaxBatchSize
	}
	if options.BatchSize < 1 || options.BatchSize > MaxAsyncBatcherBatchSize {
		return nil, ErrWorkerLocalExchangeBatchSizeInvalid
	}
	if options.Capacity/options.Partitions < options.BatchSize {
		return nil, ErrWorkerLocalExchangeCapacityInvalid
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
	bufferPool := &sync.Pool{
		New: func() any {
			return make([]T, 0, options.BatchSize)
		},
	}

	batchers := make([]*AsyncBatcher[T], options.Partitions)
	baseCapacity := options.Capacity / options.Partitions
	extraCapacity := options.Capacity % options.Partitions
	for partition := range batchers {
		valueCapacity := baseCapacity
		if partition < extraCapacity {
			valueCapacity++
		}
		capacity := valueCapacity / options.BatchSize
		partition := partition
		current, err := NewAsyncBatcher(AsyncBatcherOptions[T]{
			Capacity:      capacity,
			MaxBatchSize:  options.BatchSize,
			FlushInterval: options.FlushInterval,
			Context:       options.Context,
			Handler: func(ctx context.Context, batch []T) error {
				defer func() {
					clear(batch)
					bufferPool.Put(batch[:0])
				}()
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

	return &WorkerLocalExchange[T]{
		batchers:   batchers,
		buffers:    make([][]T, options.Workers*options.Partitions),
		bufferPool: bufferPool,
		workers:    options.Workers,
		partitions: options.Partitions,
		batchSize:  options.BatchSize,
	}, nil
}

// WorkerCount returns the configured number of producer worker slots. A nil
// receiver returns zero.
func (exchange *WorkerLocalExchange[T]) WorkerCount() int {
	if exchange == nil {
		return 0
	}
	return exchange.workers
}

// PartitionCount returns the number of independent partition workers. A nil
// receiver returns zero.
func (exchange *WorkerLocalExchange[T]) PartitionCount() int {
	if exchange == nil {
		return 0
	}
	return exchange.partitions
}

// Submit appends one value to a worker's private partition buffer. Full
// buffers are transferred as one batch. A failed transfer leaves the buffer
// intact so the caller can retry with a live context or call Flush later.
func (exchange *WorkerLocalExchange[T]) Submit(worker, partition int, ctx context.Context, value T) error {
	if exchange == nil {
		return ErrWorkerLocalExchangeNil
	}
	if worker < 0 || worker >= exchange.workers {
		exchange.rejected.Add(1)
		return ErrWorkerLocalExchangeWorkerInvalid
	}
	if partition < 0 || partition >= exchange.partitions {
		exchange.rejected.Add(1)
		return ErrWorkerLocalExchangePartitionInvalid
	}
	if exchange.closed.Load() {
		exchange.rejected.Add(1)
		return ErrWorkerLocalExchangeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		exchange.rejected.Add(1)
		return err
	}

	bufferIndex := exchange.bufferIndex(worker, partition)
	buffer := exchange.buffers[bufferIndex]
	if len(buffer) == exchange.batchSize {
		if err := exchange.flushBuffer(bufferIndex, partition, ctx); err != nil {
			return err
		}
		buffer = nil
	}
	if buffer == nil {
		buffer = exchange.bufferPool.Get().([]T)[:0]
	}
	buffer = append(buffer, value)
	exchange.buffers[bufferIndex] = buffer
	exchange.submitted.Add(1)
	exchange.buffered.Add(1)
	if len(buffer) == exchange.batchSize {
		return exchange.flushBuffer(bufferIndex, partition, ctx)
	}
	return nil
}

// FlushWorker transfers all private buffers for one worker and waits until
// every partition worker has handled the submitted batches.
func (exchange *WorkerLocalExchange[T]) FlushWorker(worker int, ctx context.Context) error {
	if exchange == nil {
		return ErrWorkerLocalExchangeNil
	}
	if worker < 0 || worker >= exchange.workers {
		return ErrWorkerLocalExchangeWorkerInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var flushErr error
	for partition := 0; partition < exchange.partitions; partition++ {
		flushErr = errors.Join(flushErr, exchange.flushBuffer(exchange.bufferIndex(worker, partition), partition, ctx))
	}
	return errors.Join(flushErr, exchange.flushPartitions(ctx, false))
}

// Flush transfers all private buffers and waits until every partition worker
// has handled the submitted batches. It is the explicit boundary for partial
// local batches; a FlushInterval only bounds already transferred batches.
func (exchange *WorkerLocalExchange[T]) Flush(ctx context.Context) error {
	if exchange == nil {
		return ErrWorkerLocalExchangeNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	flushErr := exchange.flushLocal(ctx)
	return errors.Join(flushErr, exchange.flushPartitions(ctx, false))
}

// Close stops accepting values, transfers remaining local buffers, and drains
// every partition worker. If ctx expires before local buffers are transferred,
// Close can be called again with a new context to retry them.
func (exchange *WorkerLocalExchange[T]) Close(ctx context.Context) error {
	if exchange == nil {
		return ErrWorkerLocalExchangeNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	exchange.closeMu.Lock()
	defer exchange.closeMu.Unlock()

	exchange.closed.Store(true)
	if err := exchange.flushLocal(ctx); err != nil {
		return err
	}
	return exchange.flushPartitions(ctx, true)
}

// Stats returns aggregate counters and one snapshot for each partition. The
// Partitions slice is newly allocated for every call.
func (exchange *WorkerLocalExchange[T]) Stats() WorkerLocalExchangeStats {
	if exchange == nil {
		return WorkerLocalExchangeStats{}
	}
	stats := WorkerLocalExchangeStats{
		Submitted:  exchange.submitted.Load(),
		Buffered:   int(exchange.buffered.Load()),
		Partitions: make([]AsyncBatcherStats, len(exchange.batchers)),
	}
	for partition, batcher := range exchange.batchers {
		partStats := batcher.Stats()
		stats.Partitions[partition] = partStats
		stats.FlushedBatches += partStats.FlushedBatches
		stats.FlushedItems += partStats.FlushedItems
		stats.HandlerErrors += partStats.HandlerErrors
		stats.Rejected += partStats.Rejected
		stats.Pending += partStats.Pending
	}
	stats.Rejected += exchange.rejected.Load()
	stats.Pending += stats.Buffered
	return stats
}

func (exchange *WorkerLocalExchange[T]) bufferIndex(worker, partition int) int {
	return worker*exchange.partitions + partition
}

func (exchange *WorkerLocalExchange[T]) flushBuffer(bufferIndex, partition int, ctx context.Context) error {
	values := exchange.buffers[bufferIndex]
	if len(values) == 0 {
		return nil
	}
	if err := exchange.batchers[partition].SubmitBatch(ctx, values); err != nil {
		return err
	}
	exchange.buffers[bufferIndex] = nil
	exchange.buffered.Add(-int64(len(values)))
	return nil
}

func (exchange *WorkerLocalExchange[T]) flushLocal(ctx context.Context) error {
	var flushErr error
	for worker := 0; worker < exchange.workers; worker++ {
		for partition := 0; partition < exchange.partitions; partition++ {
			flushErr = errors.Join(flushErr, exchange.flushBuffer(exchange.bufferIndex(worker, partition), partition, ctx))
		}
	}
	return flushErr
}

func (exchange *WorkerLocalExchange[T]) flushPartitions(ctx context.Context, closeWorkers bool) error {
	errorsByPartition := make([]error, len(exchange.batchers))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(exchange.batchers))
	for partition, batcher := range exchange.batchers {
		go func(partition int, batcher *AsyncBatcher[T]) {
			defer waitGroup.Done()
			if closeWorkers {
				errorsByPartition[partition] = batcher.Close(ctx)
				return
			}
			errorsByPartition[partition] = batcher.Flush(ctx)
		}(partition, batcher)
	}
	waitGroup.Wait()
	return errors.Join(errorsByPartition...)
}
