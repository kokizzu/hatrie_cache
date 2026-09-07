package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrOrderedMapInvalid reports invalid worker or callback configuration.
var ErrOrderedMapInvalid = errors.New("hatPipeline: invalid ordered map")

// OrderedMapFunc transforms one input value.
type OrderedMapFunc[T, U any] func(context.Context, T) (U, error)

// OrderedMap processes input concurrently and returns results in the same
// order as input. Workers claim indexes atomically and write only to their
// own result slot, so the merge needs no sorting or result map. The first
// processing error cancels remaining cooperative work.
func OrderedMap[T, U any](ctx context.Context, input []T, workers int, process OrderedMapFunc[T, U]) ([]U, error) {
	if workers <= 0 || process == nil {
		return nil, ErrOrderedMapInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input) == 0 {
		return nil, nil
	}
	if workers > len(input) {
		workers = len(input)
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]U, len(input))
	var next atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for range workers {
		go func() {
			defer waitGroup.Done()
			for {
				if workCtx.Err() != nil {
					return
				}
				index := int(next.Add(1) - 1)
				if index >= len(input) {
					return
				}
				result, err := process(workCtx, input[index])
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				results[index] = result
			}
		}()
	}
	waitGroup.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
