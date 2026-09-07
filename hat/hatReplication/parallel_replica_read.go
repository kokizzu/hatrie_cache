package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrParallelReplicaReadInvalid reports invalid replica-read input.
	ErrParallelReplicaReadInvalid = errors.New("parallel replica read is invalid")
	// ErrParallelReplicaReadFailed reports that every started replica failed.
	ErrParallelReplicaReadFailed = errors.New("parallel replica read failed")
)

// ParallelReplicaReadFunc executes one query against one named replica. It
// should stop promptly when ctx is canceled after another replica succeeds.
type ParallelReplicaReadFunc func(context.Context, string) (any, error)

// ParallelReplicaReadAttempt records the observable state of one replica
// attempt. Attempts are returned in the same order as the input nodes.
type ParallelReplicaReadAttempt struct {
	Node      string
	Started   bool
	Completed bool
	Succeeded bool
	Error     string
}

// ParallelReplicaReadResult is the first successful value and the attempts
// known when the executor returned. Started but incomplete losers are retained
// in the report so callers can inspect hedging behavior.
type ParallelReplicaReadResult struct {
	Node     string
	Value    any
	Attempts []ParallelReplicaReadAttempt
}

type parallelReplicaReadOutcome struct {
	index int
	value any
	err   error
}

// ExecuteParallelReplicaRead executes a read against replicas until one
// succeeds. The first replica starts immediately. With hedgeDelay == 0 all
// replicas start immediately; with a positive delay one more replica starts
// after each interval while no read has succeeded. A successful read cancels
// the remaining attempts. If every started replica fails, the next hedge is
// still launched until all replicas have been tried.
func ExecuteParallelReplicaRead(ctx context.Context, nodes []string, hedgeDelay time.Duration, read ParallelReplicaReadFunc) (ParallelReplicaReadResult, error) {
	if read == nil || hedgeDelay < 0 || len(nodes) == 0 {
		return ParallelReplicaReadResult{}, ErrParallelReplicaReadInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ParallelReplicaReadResult{}, err
	}
	normalized, err := normalizeParallelReplicaNodes(nodes)
	if err != nil {
		return ParallelReplicaReadResult{}, err
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan parallelReplicaReadOutcome, len(normalized))
	attempts := make([]ParallelReplicaReadAttempt, len(normalized))
	for index, node := range normalized {
		attempts[index].Node = node
	}
	launched := 0
	completed := 0
	launch := func(index int) {
		attempts[index].Started = true
		launched++
		go func() {
			value, err := read(readCtx, normalized[index])
			outcomes <- parallelReplicaReadOutcome{index: index, value: value, err: err}
		}()
	}
	launch(0)
	next := 1

	var timer *time.Timer
	var timerC <-chan time.Time
	if hedgeDelay == 0 {
		for next < len(normalized) {
			launch(next)
			next++
		}
	} else if next < len(normalized) {
		timer = time.NewTimer(hedgeDelay)
		timerC = timer.C
		defer timer.Stop()
	}

	for {
		if completed == launched && next == len(normalized) {
			return ParallelReplicaReadResult{Attempts: cloneParallelReplicaReadAttempts(attempts)}, fmt.Errorf("%w: all %d replicas failed", ErrParallelReplicaReadFailed, len(normalized))
		}
		select {
		case <-ctx.Done():
			return ParallelReplicaReadResult{}, ctx.Err()
		case <-timerC:
			launch(next)
			next++
			if next < len(normalized) {
				timer.Reset(hedgeDelay)
			} else {
				timerC = nil
			}
		case outcome := <-outcomes:
			completed++
			attempt := &attempts[outcome.index]
			attempt.Completed = true
			if outcome.err != nil {
				attempt.Error = outcome.err.Error()
				if completed == launched && next < len(normalized) {
					launch(next)
					next++
					if next < len(normalized) {
						resetParallelReplicaTimer(timer, hedgeDelay)
					} else {
						timerC = nil
					}
				}
				continue
			}
			attempt.Succeeded = true
			return ParallelReplicaReadResult{
				Node:     attempt.Node,
				Value:    outcome.value,
				Attempts: cloneParallelReplicaReadAttempts(attempts),
			}, nil
		}
	}
}

func resetParallelReplicaTimer(timer *time.Timer, delay time.Duration) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func normalizeParallelReplicaNodes(nodes []string) ([]string, error) {
	normalized := make([]string, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return nil, ErrParallelReplicaReadInvalid
		}
		if _, exists := seen[node]; exists {
			return nil, ErrParallelReplicaReadInvalid
		}
		seen[node] = struct{}{}
		normalized[index] = node
	}
	return normalized, nil
}

func cloneParallelReplicaReadAttempts(attempts []ParallelReplicaReadAttempt) []ParallelReplicaReadAttempt {
	if len(attempts) == 0 {
		return nil
	}
	cloned := make([]ParallelReplicaReadAttempt, len(attempts))
	copy(cloned, attempts)
	return cloned
}
