package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultParallelReplicaRangeMaxConcurrency bounds concurrent range reads
	// when the caller does not provide an explicit limit.
	DefaultParallelReplicaRangeMaxConcurrency = 8
	// MaxParallelReplicaRangeConcurrency prevents accidental unbounded fanout.
	MaxParallelReplicaRangeConcurrency = 256
)

var (
	// ErrParallelReplicaRangeReadContextNil reports a nil context.
	ErrParallelReplicaRangeReadContextNil = errors.New("parallel replica range read context is nil")
	// ErrParallelReplicaRangeReadInvalid reports invalid range-read input.
	ErrParallelReplicaRangeReadInvalid = errors.New("parallel replica range read is invalid")
	// ErrParallelReplicaRangeReadFailed reports a range that failed on every
	// eligible replica.
	ErrParallelReplicaRangeReadFailed = errors.New("parallel replica range read failed")
)

// ParallelReplicaRange is one independently readable portion of a scan.
// Payload is owned by the caller and is passed unchanged to the read function.
type ParallelReplicaRange struct {
	ID      string
	Payload any
}

// ParallelReplicaRangeReadFunc reads one range from one replica. It should
// stop promptly when ctx is canceled after another range fails.
type ParallelReplicaRangeReadFunc func(context.Context, string, ParallelReplicaRange) (any, error)

// ParallelReplicaRangeReadOptions controls bounded range fanout. Zero
// MaxConcurrency uses DefaultParallelReplicaRangeMaxConcurrency.
type ParallelReplicaRangeReadOptions struct {
	MaxConcurrency int
	RecordAttempts bool
}

// ParallelReplicaRangeAttempt records one replica attempt for a range.
type ParallelReplicaRangeAttempt struct {
	Node      string
	Started   bool
	Completed bool
	Succeeded bool
	Error     string
}

// ParallelReplicaRangeResult contains one successful range result. Results
// are returned in the same order as the input ranges, not completion order.
type ParallelReplicaRangeResult struct {
	ID       string
	Node     string
	Value    any
	Attempts []ParallelReplicaRangeAttempt
}

// ParallelReplicaRangeReadResult contains all successful range results.
// Callers can merge Value fields in range order without duplicate reads.
type ParallelReplicaRangeReadResult struct {
	Ranges []ParallelReplicaRangeResult
}

type parallelReplicaRangeReadJobResult struct {
	index  int
	result ParallelReplicaRangeResult
	err    error
}

// ExecuteParallelReplicaRangeRead assigns each range to one replica, runs
// ranges concurrently under MaxConcurrency, and retries a failed range on the
// next replicas in deterministic round-robin order. A successful range is
// never read from a second replica. If any range exhausts its replicas, the
// coordinator cancels outstanding work and returns no partial result.
func ExecuteParallelReplicaRangeRead(ctx context.Context, nodes []string, ranges []ParallelReplicaRange, options ParallelReplicaRangeReadOptions, read ParallelReplicaRangeReadFunc) (ParallelReplicaRangeReadResult, error) {
	if ctx == nil {
		return ParallelReplicaRangeReadResult{}, ErrParallelReplicaRangeReadContextNil
	}
	if read == nil || len(nodes) == 0 || len(ranges) == 0 || options.MaxConcurrency < 0 || options.MaxConcurrency > MaxParallelReplicaRangeConcurrency {
		return ParallelReplicaRangeReadResult{}, ErrParallelReplicaRangeReadInvalid
	}
	if err := ctx.Err(); err != nil {
		return ParallelReplicaRangeReadResult{}, err
	}
	normalizedNodes, err := normalizeParallelReplicaNodes(nodes)
	if err != nil {
		return ParallelReplicaRangeReadResult{}, fmt.Errorf("%w: replica nodes: %v", ErrParallelReplicaRangeReadInvalid, err)
	}
	normalizedRanges, err := normalizeParallelReplicaRanges(ranges)
	if err != nil {
		return ParallelReplicaRangeReadResult{}, err
	}
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = DefaultParallelReplicaRangeMaxConcurrency
	}
	if options.MaxConcurrency > len(normalizedRanges) {
		options.MaxConcurrency = len(normalizedRanges)
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make([]ParallelReplicaRangeResult, len(normalizedRanges))
	var workers sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for worker := 0; worker < options.MaxConcurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				result, readErr := executeParallelReplicaRange(readCtx, normalizedNodes, normalizedRanges[index], index, options.RecordAttempts, read)
				if readErr != nil {
					errOnce.Do(func() {
						firstErr = readErr
						cancel()
					})
					continue
				}
				results[index] = result
			}
		}()
	}
	for index := range normalizedRanges {
		select {
		case jobs <- index:
		case <-readCtx.Done():
			break
		}
		if readCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return ParallelReplicaRangeReadResult{}, err
	}
	if firstErr != nil {
		return ParallelReplicaRangeReadResult{}, firstErr
	}
	return ParallelReplicaRangeReadResult{Ranges: results}, nil
}

func executeParallelReplicaRange(ctx context.Context, nodes []string, item ParallelReplicaRange, rangeIndex int, recordAttempts bool, read ParallelReplicaRangeReadFunc) (ParallelReplicaRangeResult, error) {
	result := ParallelReplicaRangeResult{ID: item.ID}
	var attempts []ParallelReplicaRangeAttempt
	if recordAttempts {
		attempts = make([]ParallelReplicaRangeAttempt, len(nodes))
	}
	var lastErr error
	for attemptIndex := range nodes {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		node := nodes[(rangeIndex+attemptIndex)%len(nodes)]
		if recordAttempts {
			attempts[attemptIndex].Node = node
			attempts[attemptIndex].Started = true
		}
		value, err := read(ctx, node, item)
		if recordAttempts {
			attempts[attemptIndex].Completed = true
		}
		if err == nil {
			if recordAttempts {
				attempts[attemptIndex].Succeeded = true
			}
			result.Node = node
			result.Value = value
			result.Attempts = attempts
			return result, nil
		}
		if recordAttempts {
			attempts[attemptIndex].Error = err.Error()
		}
		lastErr = err
	}
	result.Attempts = attempts
	return result, fmt.Errorf("%w: range %q exhausted replicas: %v", ErrParallelReplicaRangeReadFailed, item.ID, lastErr)
}

func normalizeParallelReplicaRanges(ranges []ParallelReplicaRange) ([]ParallelReplicaRange, error) {
	normalized := make([]ParallelReplicaRange, len(ranges))
	seen := make(map[string]struct{}, len(ranges))
	for index, item := range ranges {
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			return nil, ErrParallelReplicaRangeReadInvalid
		}
		if _, exists := seen[item.ID]; exists {
			return nil, ErrParallelReplicaRangeReadInvalid
		}
		seen[item.ID] = struct{}{}
		normalized[index] = item
	}
	return normalized, nil
}
