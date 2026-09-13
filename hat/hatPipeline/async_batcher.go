package hatPipeline

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrAsyncBatcherNil indicates that a method was called on a nil batcher.
	ErrAsyncBatcherNil = errors.New("hatPipeline: async batcher is nil")
	// ErrAsyncBatcherHandlerRequired indicates that no batch handler was supplied.
	ErrAsyncBatcherHandlerRequired = errors.New("hatPipeline: async batch handler is required")
	// ErrAsyncBatcherCapacityInvalid indicates an invalid queue capacity.
	ErrAsyncBatcherCapacityInvalid = errors.New("hatPipeline: async batcher capacity is invalid")
	// ErrAsyncBatcherMaxBatchSizeInvalid indicates an invalid batch size.
	ErrAsyncBatcherMaxBatchSizeInvalid = errors.New("hatPipeline: async batcher max batch size is invalid")
	// ErrAsyncBatcherFlushIntervalInvalid indicates an invalid flush interval.
	ErrAsyncBatcherFlushIntervalInvalid = errors.New("hatPipeline: async batcher flush interval is invalid")
	// ErrAsyncBatcherAdaptiveTargetInvalid indicates an invalid adaptive target.
	ErrAsyncBatcherAdaptiveTargetInvalid = errors.New("hatPipeline: async batcher adaptive target is invalid")
	// ErrAsyncBatcherAdaptiveMinIntervalInvalid indicates an invalid adaptive lower bound.
	ErrAsyncBatcherAdaptiveMinIntervalInvalid = errors.New("hatPipeline: async batcher adaptive minimum interval is invalid")
	// ErrAsyncBatcherAdaptiveMaxIntervalInvalid indicates an invalid adaptive upper bound.
	ErrAsyncBatcherAdaptiveMaxIntervalInvalid = errors.New("hatPipeline: async batcher adaptive maximum interval is invalid")
	// ErrAsyncBatcherClosed indicates that the batcher no longer accepts items.
	ErrAsyncBatcherClosed = errors.New("hatPipeline: async batcher is closed")
)

const (
	// DefaultAsyncBatcherCapacity bounds queued items when Capacity is zero.
	DefaultAsyncBatcherCapacity = 1024
	// DefaultAsyncBatcherMaxBatchSize is used when MaxBatchSize is zero.
	DefaultAsyncBatcherMaxBatchSize = 64
	// DefaultAsyncBatcherFlushInterval bounds latency when FlushInterval is zero.
	DefaultAsyncBatcherFlushInterval = 10 * time.Millisecond
	// MaxAsyncBatcherCapacity prevents accidental unbounded queue allocation.
	MaxAsyncBatcherCapacity = 1 << 20
	// MaxAsyncBatcherBatchSize bounds one handler call and its retained input.
	MaxAsyncBatcherBatchSize = 4096
)

// AsyncBatcherOptions configures an opt-in bounded asynchronous batcher.
// Capacity, MaxBatchSize, and FlushInterval use sane defaults when zero. A
// negative value is rejected. AdaptiveFlush is disabled by default; when it is
// enabled, the worker adjusts its timeout from observed arrival rate while
// staying within the configured adaptive interval bounds. Handler runs
// serially, in submission order, and must not retain or mutate the values slice
// after returning.
type AsyncBatcherOptions[T any] struct {
	Capacity                 int
	MaxBatchSize             int
	FlushInterval            time.Duration
	AdaptiveFlush            bool
	AdaptiveTargetBatchSize  int
	AdaptiveMinFlushInterval time.Duration
	AdaptiveMaxFlushInterval time.Duration
	Context                  context.Context
	Handler                  func(context.Context, []T) error
}

// AsyncBatcherStats is a point-in-time view of batcher activity. Pending
// includes values queued or currently being handled.
type AsyncBatcherStats struct {
	Submitted      uint64
	FlushedBatches uint64
	FlushedItems   uint64
	HandlerErrors  uint64
	Rejected       uint64
	Pending        int
}

const (
	asyncBatcherItem uint8 = iota
	asyncBatcherFlush
)

type asyncBatcherRequest[T any] struct {
	kind   uint8
	value  T
	result chan error
}

// AsyncBatcher buffers values and invokes one handler for bounded batches.
// It is useful for ClickHouse-style asynchronous inserts: Submit applies
// backpressure at Capacity, MaxBatchSize controls throughput, and Flush or
// Close provides an explicit acknowledgement boundary. It is disabled unless
// the caller constructs one.
type AsyncBatcher[T any] struct {
	requests chan asyncBatcherRequest[T]
	done     chan struct{}
	closeCh  chan struct{}

	stateMu  sync.Mutex
	closed   bool
	closeErr error

	context          context.Context
	handler          func(context.Context, []T) error
	maxSize          int
	interval         time.Duration
	adaptive         bool
	adaptiveTarget   int
	adaptiveMin      time.Duration
	adaptiveMax      time.Duration
	adaptiveInterval time.Duration
	arrivalRate      float64
	lastArrival      time.Time
	arrivalSamples   int

	submitted      atomic.Uint64
	flushedBatches atomic.Uint64
	flushedItems   atomic.Uint64
	handlerErrors  atomic.Uint64
	rejected       atomic.Uint64
	pending        atomic.Int64
}

// NewAsyncBatcher creates a running bounded batcher. The handler is called by
// one internal worker, so a handler error is retained and returned by the
// next Flush or Close call. The worker continues accepting later batches.
func NewAsyncBatcher[T any](options AsyncBatcherOptions[T]) (*AsyncBatcher[T], error) {
	if options.Handler == nil {
		return nil, ErrAsyncBatcherHandlerRequired
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultAsyncBatcherCapacity
	}
	if options.Capacity < 1 || options.Capacity > MaxAsyncBatcherCapacity {
		return nil, ErrAsyncBatcherCapacityInvalid
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
	adaptiveTarget, adaptiveMin, adaptiveMax := 0, time.Duration(0), time.Duration(0)
	if options.AdaptiveFlush {
		adaptiveTarget = options.AdaptiveTargetBatchSize
		if adaptiveTarget == 0 {
			adaptiveTarget = options.MaxBatchSize / 2
			if adaptiveTarget == 0 {
				adaptiveTarget = 1
			}
		}
		if adaptiveTarget < 1 || adaptiveTarget > options.MaxBatchSize {
			return nil, ErrAsyncBatcherAdaptiveTargetInvalid
		}
		adaptiveMin = options.AdaptiveMinFlushInterval
		if adaptiveMin == 0 {
			adaptiveMin = options.FlushInterval / 4
			if adaptiveMin == 0 {
				adaptiveMin = time.Nanosecond
			}
		}
		if adaptiveMin <= 0 {
			return nil, ErrAsyncBatcherAdaptiveMinIntervalInvalid
		}
		adaptiveMax = options.AdaptiveMaxFlushInterval
		if adaptiveMax == 0 {
			if options.FlushInterval > time.Duration(math.MaxInt64)/4 {
				adaptiveMax = time.Duration(math.MaxInt64)
			} else {
				adaptiveMax = options.FlushInterval * 4
			}
		}
		if adaptiveMax <= 0 || adaptiveMax < adaptiveMin {
			return nil, ErrAsyncBatcherAdaptiveMaxIntervalInvalid
		}
	}
	if options.Context == nil {
		options.Context = context.Background()
	}
	batcher := &AsyncBatcher[T]{
		requests:         make(chan asyncBatcherRequest[T], options.Capacity),
		done:             make(chan struct{}),
		closeCh:          make(chan struct{}),
		context:          options.Context,
		handler:          options.Handler,
		maxSize:          options.MaxBatchSize,
		interval:         options.FlushInterval,
		adaptive:         options.AdaptiveFlush,
		adaptiveTarget:   adaptiveTarget,
		adaptiveMin:      adaptiveMin,
		adaptiveMax:      adaptiveMax,
		adaptiveInterval: clampAsyncBatcherInterval(options.FlushInterval, adaptiveMin, adaptiveMax),
	}
	go batcher.run()
	return batcher, nil
}

// Submit queues one value, waiting for capacity or ctx cancellation. A
// successful Submit is eventually included in exactly one handler batch.
func (batcher *AsyncBatcher[T]) Submit(ctx context.Context, value T) error {
	if batcher == nil {
		return ErrAsyncBatcherNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	batcher.stateMu.Lock()
	if batcher.closed {
		batcher.stateMu.Unlock()
		batcher.rejected.Add(1)
		return ErrAsyncBatcherClosed
	}
	batcher.pending.Add(1)
	request := asyncBatcherRequest[T]{kind: asyncBatcherItem, value: value}
	select {
	case batcher.requests <- request:
		batcher.submitted.Add(1)
		batcher.stateMu.Unlock()
		return nil
	case <-ctx.Done():
		batcher.pending.Add(-1)
		batcher.rejected.Add(1)
		batcher.stateMu.Unlock()
		return ctx.Err()
	}
}

// Flush waits until all values submitted before the flush marker have been
// handled. Handler errors since the previous Flush or Close are returned.
func (batcher *AsyncBatcher[T]) Flush(ctx context.Context) error {
	if batcher == nil {
		return ErrAsyncBatcherNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	batcher.stateMu.Lock()
	if batcher.closed {
		done := batcher.done
		batcher.stateMu.Unlock()
		select {
		case <-done:
			return batcher.closeErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := make(chan error, 1)
	request := asyncBatcherRequest[T]{kind: asyncBatcherFlush, result: result}
	select {
	case batcher.requests <- request:
		batcher.stateMu.Unlock()
	case <-ctx.Done():
		batcher.stateMu.Unlock()
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close drains and flushes all accepted values. It is safe to call multiple
// times; later calls observe the first close result. If ctx expires while the
// worker is draining, Close returns ctx.Err but the worker continues closing.
func (batcher *AsyncBatcher[T]) Close(ctx context.Context) error {
	if batcher == nil {
		return ErrAsyncBatcherNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	batcher.stateMu.Lock()
	if batcher.closed {
		done := batcher.done
		batcher.stateMu.Unlock()
		select {
		case <-done:
			return batcher.closeErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	batcher.closed = true
	closeCh := batcher.closeCh
	batcher.stateMu.Unlock()
	close(closeCh)
	select {
	case <-batcher.done:
		return batcher.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns counters without stopping or flushing the batcher.
func (batcher *AsyncBatcher[T]) Stats() AsyncBatcherStats {
	if batcher == nil {
		return AsyncBatcherStats{}
	}
	return AsyncBatcherStats{
		Submitted:      batcher.submitted.Load(),
		FlushedBatches: batcher.flushedBatches.Load(),
		FlushedItems:   batcher.flushedItems.Load(),
		HandlerErrors:  batcher.handlerErrors.Load(),
		Rejected:       batcher.rejected.Load(),
		Pending:        int(batcher.pending.Load()),
	}
}

func (batcher *AsyncBatcher[T]) run() {
	if batcher.adaptive {
		batcher.runAdaptive()
		return
	}
	batcher.runFixed()
}

func (batcher *AsyncBatcher[T]) runFixed() {
	timer := time.NewTimer(batcher.interval)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time
	startTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batcher.interval)
		timerC = timer.C
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}

	batch := make([]T, 0, batcher.maxSize)
	var pendingErr error
	for {
		if batcher.closeRequested() {
			for {
				select {
				case request := <-batcher.requests:
					if request.kind == asyncBatcherItem {
						if len(batch) == 0 {
							startTimer()
						}
						batch = append(batch, request.value)
						if len(batch) == batcher.maxSize {
							pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
							batch = batch[:0]
							stopTimer()
						}
						continue
					}
					if request.kind == asyncBatcherFlush {
						if len(batch) > 0 {
							pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
							batch = batch[:0]
						}
						stopTimer()
						err := pendingErr
						pendingErr = nil
						request.result <- err
						continue
					}
				default:
					if len(batch) > 0 {
						pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
						batch = batch[:0]
					}
					stopTimer()
					batcher.finishClose(pendingErr)
					return
				}
			}
		}
		select {
		case request := <-batcher.requests:
			switch request.kind {
			case asyncBatcherItem:
				if len(batch) == 0 {
					startTimer()
				}
				batch = append(batch, request.value)
				if len(batch) < batcher.maxSize {
					continue
				}
				pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
				batch = batch[:0]
				stopTimer()
			case asyncBatcherFlush:
				if len(batch) > 0 {
					pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
					batch = batch[:0]
				}
				stopTimer()
				err := pendingErr
				pendingErr = nil
				request.result <- err
			}
		case <-timerC:
			pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
			batch = batch[:0]
			stopTimer()
		case <-batcher.closeCh:
			// The next loop drains accepted requests before closing.
		}
	}
}

func (batcher *AsyncBatcher[T]) runAdaptive() {
	timer := time.NewTimer(batcher.adaptiveInterval)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time
	startTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batcher.adaptiveInterval)
		timerC = timer.C
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}

	batch := make([]T, 0, batcher.maxSize)
	var pendingErr error
	for {
		if batcher.closeRequested() {
			for {
				select {
				case request := <-batcher.requests:
					if request.kind == asyncBatcherItem {
						batcher.observeAdaptiveArrival()
						if len(batch) == 0 {
							startTimer()
						}
						batch = append(batch, request.value)
						if len(batch) == batcher.maxSize {
							pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
							batch = batch[:0]
							stopTimer()
						}
						continue
					}
					if request.kind == asyncBatcherFlush {
						if len(batch) > 0 {
							pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
							batch = batch[:0]
						}
						stopTimer()
						err := pendingErr
						pendingErr = nil
						request.result <- err
						continue
					}
				default:
					if len(batch) > 0 {
						pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
						batch = batch[:0]
					}
					stopTimer()
					batcher.finishClose(pendingErr)
					return
				}
			}
		}
		select {
		case request := <-batcher.requests:
			switch request.kind {
			case asyncBatcherItem:
				batcher.observeAdaptiveArrival()
				if len(batch) == 0 {
					startTimer()
				}
				batch = append(batch, request.value)
				if len(batch) < batcher.maxSize {
					continue
				}
				pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
				batch = batch[:0]
				stopTimer()
			case asyncBatcherFlush:
				if len(batch) > 0 {
					pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
					batch = batch[:0]
				}
				stopTimer()
				err := pendingErr
				pendingErr = nil
				request.result <- err
			}
		case <-timerC:
			pendingErr = errors.Join(pendingErr, batcher.flushBatch(batch))
			batch = batch[:0]
			stopTimer()
		case <-batcher.closeCh:
			// The next loop drains accepted requests before closing.
		}
	}
}

func (batcher *AsyncBatcher[T]) observeArrival(now time.Time) {
	batcher.observeArrivalCount(now, 1)
}

const asyncBatcherAdaptiveArrivalSampleSize = 8

func (batcher *AsyncBatcher[T]) observeAdaptiveArrival() {
	if batcher == nil || !batcher.adaptive {
		return
	}
	batcher.arrivalSamples++
	if batcher.lastArrival.IsZero() || batcher.arrivalSamples >= asyncBatcherAdaptiveArrivalSampleSize {
		now := time.Now()
		batcher.observeArrivalCount(now, batcher.arrivalSamples)
		batcher.arrivalSamples = 0
	}
}

func (batcher *AsyncBatcher[T]) observeArrivalCount(now time.Time, samples int) {
	if batcher == nil || !batcher.adaptive {
		return
	}
	if samples < 1 {
		return
	}
	if batcher.lastArrival.IsZero() {
		batcher.lastArrival = now
		return
	}
	elapsed := now.Sub(batcher.lastArrival)
	batcher.lastArrival = now
	if elapsed <= 0 {
		return
	}
	rate := float64(samples) * float64(time.Second) / float64(elapsed)
	if batcher.arrivalRate == 0 {
		batcher.arrivalRate = rate
	} else {
		batcher.arrivalRate = (batcher.arrivalRate*3 + rate) / 4
	}
	batcher.adaptiveInterval = asyncBatcherAdaptiveInterval(
		batcher.interval,
		batcher.adaptiveTarget,
		batcher.arrivalRate,
		batcher.adaptiveMin,
		batcher.adaptiveMax,
	)
}

func asyncBatcherAdaptiveInterval(base time.Duration, target int, rate float64, min, max time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultAsyncBatcherFlushInterval
	}
	if min <= 0 {
		min = time.Nanosecond
	}
	if max < min {
		max = min
	}
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || target <= 0 {
		return clampAsyncBatcherInterval(base, min, max)
	}
	nanos := float64(target) * float64(time.Second) / rate
	if nanos <= float64(min) {
		return min
	}
	if nanos >= float64(max) {
		return max
	}
	return time.Duration(nanos)
}

func clampAsyncBatcherInterval(value, min, max time.Duration) time.Duration {
	if min > 0 && value < min {
		return min
	}
	if max > 0 && value > max {
		return max
	}
	return value
}

func (batcher *AsyncBatcher[T]) closeRequested() bool {
	select {
	case <-batcher.closeCh:
		return true
	default:
		return false
	}
}

func (batcher *AsyncBatcher[T]) finishClose(err error) {
	batcher.stateMu.Lock()
	batcher.closeErr = err
	batcher.stateMu.Unlock()
	close(batcher.done)
}

func (batcher *AsyncBatcher[T]) flushBatch(batch []T) error {
	if len(batch) == 0 {
		return nil
	}
	batcher.flushedBatches.Add(1)
	batcher.flushedItems.Add(uint64(len(batch)))
	err := batcher.handler(batcher.context, batch)
	batcher.pending.Add(-int64(len(batch)))
	if err != nil {
		batcher.handlerErrors.Add(1)
	}
	return err
}
