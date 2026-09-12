package hatTopology

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrReplicaHedgeContextRequired indicates that ExecuteReplicaHedged was
	// called without a context.
	ErrReplicaHedgeContextRequired = errors.New("hatTopology: replica hedge context is required")
	// ErrReplicaHedgeCallRequired indicates that no replica call was provided.
	ErrReplicaHedgeCallRequired = errors.New("hatTopology: replica hedge call is required")
	// ErrReplicaHedgeCandidatesRequired indicates that no eligible candidates
	// were provided.
	ErrReplicaHedgeCandidatesRequired = errors.New("hatTopology: replica hedge candidates are required")
	// ErrReplicaHedgeAttemptsInvalid indicates an out-of-range attempt limit.
	ErrReplicaHedgeAttemptsInvalid = errors.New("hatTopology: replica hedge maximum attempts is invalid")
	// ErrReplicaHedgeDelayInvalid indicates an out-of-range hedge delay.
	ErrReplicaHedgeDelayInvalid = errors.New("hatTopology: replica hedge delay is invalid")
	// ErrReplicaHedgeAllFailed indicates that every started candidate failed.
	ErrReplicaHedgeAllFailed = errors.New("hatTopology: all replica hedge attempts failed")
)

const (
	// DefaultReplicaHedgeMaxAttempts starts the preferred candidate and one
	// bounded fallback when hedging is enabled.
	DefaultReplicaHedgeMaxAttempts = 2
	// MaxReplicaHedgeAttempts prevents an accidental fan-out from one read.
	MaxReplicaHedgeAttempts = 4
	// DefaultReplicaHedgeDelay leaves a short window for the preferred replica
	// before adding duplicate read load.
	DefaultReplicaHedgeDelay = 5 * time.Millisecond
	// MaxReplicaHedgeDelay keeps an invalid configuration from waiting without
	// bound for a fallback.
	MaxReplicaHedgeDelay = time.Hour
)

// ReplicaHedgePolicyOptions configures bounded, read-only request hedging.
// Zero values select conservative defaults. Hedging is never applied by the
// topology router automatically; callers opt in by executing this policy.
type ReplicaHedgePolicyOptions struct {
	MaxAttempts int
	HedgeDelay  time.Duration
	Observer    ReplicaHedgeObserver
}

// ReplicaHedgePolicy is an immutable validated request-hedging policy.
type ReplicaHedgePolicy struct {
	maxAttempts int
	hedgeDelay  time.Duration
	observer    ReplicaHedgeObserver
}

// ReplicaHedgeEvent describes a fallback candidate that was started after
// the preferred candidate had not completed. CandidateIndex refers to the
// caller-owned candidate slice. Delay is zero when the previous attempt
// failed before the configured hedge delay elapsed.
type ReplicaHedgeEvent struct {
	Attempt        int
	CandidateIndex int
	Delay          time.Duration
}

// ReplicaHedgeObserver receives fallback-start events synchronously from the
// coordinator. Observers should be lightweight and must not block on the
// candidate calls.
type ReplicaHedgeObserver func(event ReplicaHedgeEvent)

// ReplicaHedgeCall executes one read against a selected caller-owned
// candidate. The call must honor ctx so a winning replica can promptly cancel
// slower duplicates.
type ReplicaHedgeCall[C any, T any] func(ctx context.Context, candidate C) (T, error)

// ReplicaHedgeFailure records one failed candidate in input order.
type ReplicaHedgeFailure struct {
	CandidateIndex int
	Err            error
}

// ReplicaHedgeError reports deterministic failure details after every started
// candidate fails. Its Unwrap method preserves errors.Is/As for the sentinel
// and each underlying candidate error.
type ReplicaHedgeError struct {
	Failures []ReplicaHedgeFailure
}

func (err *ReplicaHedgeError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %d attempts", ErrReplicaHedgeAllFailed, len(err.Failures))
}

func (err *ReplicaHedgeError) Unwrap() []error {
	if err == nil {
		return nil
	}
	errorsList := make([]error, 1, len(err.Failures)+1)
	errorsList[0] = ErrReplicaHedgeAllFailed
	for _, failure := range err.Failures {
		if failure.Err != nil {
			errorsList = append(errorsList, failure.Err)
		}
	}
	return errorsList
}

// NewReplicaHedgePolicy validates and freezes one request-hedging policy.
func NewReplicaHedgePolicy(options ReplicaHedgePolicyOptions) (ReplicaHedgePolicy, error) {
	if options.MaxAttempts < 0 || options.MaxAttempts > MaxReplicaHedgeAttempts {
		return ReplicaHedgePolicy{}, ErrReplicaHedgeAttemptsInvalid
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = DefaultReplicaHedgeMaxAttempts
	}
	if options.HedgeDelay < 0 || options.HedgeDelay > MaxReplicaHedgeDelay {
		return ReplicaHedgePolicy{}, ErrReplicaHedgeDelayInvalid
	}
	if options.HedgeDelay == 0 {
		options.HedgeDelay = DefaultReplicaHedgeDelay
	}
	return ReplicaHedgePolicy{
		maxAttempts: options.MaxAttempts,
		hedgeDelay:  options.HedgeDelay,
		observer:    options.Observer,
	}, nil
}

// NewDefaultReplicaHedgePolicy returns the conservative opt-in policy.
func NewDefaultReplicaHedgePolicy() ReplicaHedgePolicy {
	policy, _ := NewReplicaHedgePolicy(ReplicaHedgePolicyOptions{})
	return policy
}

type replicaHedgeResult[T any] struct {
	index int
	value T
	err   error
}

// ExecuteReplicaHedged runs a bounded read against ordered candidates. The
// first candidate starts immediately. A later candidate starts after the
// policy delay if work is still pending, or immediately when all currently
// started attempts have failed. The first successful result wins and cancels
// the shared child context. Callers must treat the operation as read-only:
// duplicate calls may be in flight before cancellation reaches a peer.
func ExecuteReplicaHedged[C any, T any](ctx context.Context, policy ReplicaHedgePolicy, candidates []C, call ReplicaHedgeCall[C, T]) (T, int, error) {
	var zero T
	if ctx == nil {
		return zero, -1, ErrReplicaHedgeContextRequired
	}
	if call == nil {
		return zero, -1, ErrReplicaHedgeCallRequired
	}
	if len(candidates) == 0 {
		return zero, -1, ErrReplicaHedgeCandidatesRequired
	}
	if policy.maxAttempts == 0 {
		policy = NewDefaultReplicaHedgePolicy()
	}
	if err := ctx.Err(); err != nil {
		return zero, -1, err
	}

	limit := min(policy.maxAttempts, len(candidates))
	if limit == 1 {
		value, err := call(ctx, candidates[0])
		if err == nil {
			return value, 0, nil
		}
		return zero, -1, &ReplicaHedgeError{Failures: []ReplicaHedgeFailure{{CandidateIndex: 0, Err: err}}}
	}
	childContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan replicaHedgeResult[T], limit)
	var failuresByIndex []ReplicaHedgeFailure
	failedCount := 0
	started, completed := 0, 0
	start := func(index int, delay time.Duration) {
		if index > 0 && policy.observer != nil {
			policy.observer(ReplicaHedgeEvent{
				Attempt:        index + 1,
				CandidateIndex: index,
				Delay:          delay,
			})
		}
		started++
		go func() {
			value, err := call(childContext, candidates[index])
			results <- replicaHedgeResult[T]{index: index, value: value, err: err}
		}()
	}

	start(0, 0)
	var timer *time.Timer
	if started < limit {
		timer = time.NewTimer(policy.hedgeDelay)
		defer func() {
			if timer != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()
	}

	for completed < limit {
		if err := ctx.Err(); err != nil {
			return zero, -1, err
		}
		var timerChannel <-chan time.Time
		if timer != nil {
			timerChannel = timer.C
		}
		select {
		case <-ctx.Done():
			return zero, -1, ctx.Err()
		case <-timerChannel:
			timer = nil
			if started < limit {
				start(started, policy.hedgeDelay)
				if started < limit {
					timer = time.NewTimer(policy.hedgeDelay)
				}
			}
		case result := <-results:
			completed++
			if result.err == nil {
				cancel()
				return result.value, result.index, nil
			}
			if failuresByIndex == nil {
				failuresByIndex = make([]ReplicaHedgeFailure, limit)
			}
			failuresByIndex[result.index] = ReplicaHedgeFailure{CandidateIndex: result.index, Err: result.err}
			failedCount++
			if started < limit && completed == started {
				if timer != nil && !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer = nil
				start(started, 0)
				if started < limit {
					timer = time.NewTimer(policy.hedgeDelay)
				}
			}
			if completed == started && started == limit {
				failures := make([]ReplicaHedgeFailure, 0, failedCount)
				for index := 0; index < started; index++ {
					if failuresByIndex[index].Err != nil {
						failures = append(failures, failuresByIndex[index])
					}
				}
				return zero, -1, &ReplicaHedgeError{Failures: failures}
			}
		}
	}
	return zero, -1, &ReplicaHedgeError{Failures: failuresByIndex}
}
