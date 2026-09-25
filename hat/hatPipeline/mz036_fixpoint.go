package hatPipeline

import (
	"context"
	"errors"
)

const (
	// DefaultFixpointMaxSteps bounds recursive work before a non-converging
	// callback can consume unbounded CPU.
	DefaultFixpointMaxSteps = 1 << 20
	// DefaultFixpointMaxPending bounds distinct queued keys and retained values.
	DefaultFixpointMaxPending = 1 << 20
	maxFixpointLimit          = 1 << 26
)

var (
	// ErrFixpointInvalid reports a nil step callback or invalid limit.
	ErrFixpointInvalid = errors.New("hatPipeline: invalid fixpoint options")
	// ErrFixpointStepLimit reports a worklist that exceeded MaxSteps.
	ErrFixpointStepLimit = errors.New("hatPipeline: fixpoint step limit reached")
	// ErrFixpointPendingLimit reports more distinct pending keys than allowed.
	ErrFixpointPendingLimit = errors.New("hatPipeline: fixpoint pending limit reached")
)

// FixpointItem is one keyed value in a recursive dataflow.
type FixpointItem[K comparable, V any] struct {
	Key   K
	Value V
}

// FixpointEmitter adds a downstream item to the bounded worklist. A callback
// can return the error directly to stop its current step when a bound is hit.
type FixpointEmitter[K comparable, V any] func(FixpointItem[K, V]) error

// FixpointStep consumes one item and emits any newly affected items. Steps
// run serially in FIFO order, which keeps results deterministic while allowing
// callers to coalesce duplicate pending keys.
type FixpointStep[K comparable, V any] func(context.Context, FixpointItem[K, V], FixpointEmitter[K, V]) error

// FixpointOptions controls one RunFixpoint call. Zero limits use the bounded
// defaults. Merge is called only when a key is already pending; nil replaces
// the pending value with the newest emission.
type FixpointOptions[K comparable, V any] struct {
	MaxSteps   int
	MaxPending int
	Merge      func(existing, incoming V) V
	Step       FixpointStep[K, V]
}

// FixpointResult reports work performed by a converged or bounded run.
type FixpointResult struct {
	Steps      uint64
	Enqueued   uint64
	MaxPending uint64
}

// RunFixpoint drains initial through a bounded keyed worklist until no items
// remain, the context is canceled, the step callback returns an error, or a
// configured limit is reached. It does not mutate caller-owned initial data.
func RunFixpoint[K comparable, V any](ctx context.Context, initial []FixpointItem[K, V], options FixpointOptions[K, V]) (FixpointResult, error) {
	var result FixpointResult
	if options.Step == nil {
		return result, ErrFixpointInvalid
	}
	maxSteps, ok := normalizeFixpointLimit(options.MaxSteps, DefaultFixpointMaxSteps)
	if !ok {
		return result, ErrFixpointInvalid
	}
	maxPending, ok := normalizeFixpointLimit(options.MaxPending, DefaultFixpointMaxPending)
	if !ok {
		return result, ErrFixpointInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	pending := make(map[K]FixpointItem[K, V], minInt(len(initial), maxPending))
	queue := make([]K, 0, minInt(len(initial), maxPending))
	var enqueue FixpointEmitter[K, V]
	enqueue = func(item FixpointItem[K, V]) error {
		if existing, exists := pending[item.Key]; exists {
			if options.Merge != nil {
				item.Value = options.Merge(existing.Value, item.Value)
			}
			pending[item.Key] = item
			return nil
		}
		if len(pending) >= maxPending {
			return ErrFixpointPendingLimit
		}
		pending[item.Key] = item
		queue = append(queue, item.Key)
		result.Enqueued++
		if current := uint64(len(pending)); current > result.MaxPending {
			result.MaxPending = current
		}
		return nil
	}
	for _, item := range initial {
		if err := enqueue(item); err != nil {
			return result, err
		}
	}

	queueHead := 0
	for queueHead < len(queue) {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if result.Steps >= uint64(maxSteps) {
			return result, ErrFixpointStepLimit
		}
		key := queue[queueHead]
		var zeroKey K
		queue[queueHead] = zeroKey
		queueHead++
		if queueHead == len(queue) {
			queue = queue[:0]
			queueHead = 0
		}
		item, exists := pending[key]
		if !exists {
			continue
		}
		delete(pending, key)
		result.Steps++
		if err := options.Step(ctx, item, enqueue); err != nil {
			return result, err
		}
	}
	return result, nil
}

func normalizeFixpointLimit(value, fallback int) (int, bool) {
	if value == 0 {
		return fallback, true
	}
	return value, value > 0 && value <= maxFixpointLimit
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
