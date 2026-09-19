package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultRemoteCallMaxAttempts is used for retryable methods when the
	// policy leaves MaxAttempts at zero.
	DefaultRemoteCallMaxAttempts = 3
	maxRemoteCallAttempts        = 64
	maxRemoteCallBackoff         = time.Hour
)

var (
	// ErrRemoteCallContextNil indicates that a nil context was supplied.
	ErrRemoteCallContextNil = errors.New("hatReplication: remote call context is nil")
	// ErrRemoteCallFunctionNil indicates that no remote call function was supplied.
	ErrRemoteCallFunctionNil = errors.New("hatReplication: remote call function is nil")
	// ErrRemoteCallPolicyInvalid indicates invalid method, identity, attempt, or
	// backoff settings.
	ErrRemoteCallPolicyInvalid = errors.New("hatReplication: remote call policy is invalid")
	// ErrRemoteCallNonIdempotent indicates that a non-idempotent method was given
	// more than one possible attempt.
	ErrRemoteCallNonIdempotent = errors.New("hatReplication: non-idempotent remote call cannot retry")
	// ErrRemoteCallExhausted identifies a retryable call that failed on every
	// allowed attempt.
	ErrRemoteCallExhausted = errors.New("hatReplication: remote call attempts exhausted")
)

// RemoteCallMethod controls which retry safety contract is required.
type RemoteCallMethod uint8

const (
	RemoteCallRead RemoteCallMethod = iota + 1
	RemoteCallIdempotentWrite
	RemoteCallNonIdempotent
)

// RemoteCallRequest is the stable identity envelope supplied to each attempt.
// The caller must transmit IdempotencyKey and FencingToken to the peer for an
// idempotent write; the helper cannot make an endpoint deduplicate by itself.
type RemoteCallRequest struct {
	Method         RemoteCallMethod
	IdempotencyKey string
	FencingToken   uint64
	Attempt        int
}

// RemoteCallRetryEvent describes a failed attempt before the next attempt.
// IdempotencyKey is intentionally omitted so retry observers do not log it by
// default.
type RemoteCallRetryEvent struct {
	Method       RemoteCallMethod
	Attempt      int
	NextAttempt  int
	Delay        time.Duration
	FencingToken uint64
}

// RemoteCallPolicy bounds retries for one peer call. Reads may retry without
// an identity. Idempotent writes require both IdempotencyKey and a non-zero
// FencingToken. Non-idempotent methods are always limited to one attempt.
type RemoteCallPolicy struct {
	Method         RemoteCallMethod
	IdempotencyKey string
	FencingToken   uint64
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	ShouldRetry    func(error) bool
	Jitter         func(attempt int, delay time.Duration) time.Duration
	OnRetry        func(RemoteCallRetryEvent)
}

// RemoteCallError reports exhaustion after all allowed attempts failed. It
// matches ErrRemoteCallExhausted and unwraps the final attempt error.
type RemoteCallError struct {
	Attempts int
	Last     error
}

func (err RemoteCallError) Error() string {
	if err.Last == nil {
		return fmt.Sprintf("%s after %d attempts", ErrRemoteCallExhausted, err.Attempts)
	}
	return fmt.Sprintf("%s after %d attempts: %v", ErrRemoteCallExhausted, err.Attempts, err.Last)
}

func (err RemoteCallError) Is(target error) bool {
	return target == ErrRemoteCallExhausted || errors.Is(err.Last, target)
}

func (err RemoteCallError) Unwrap() error {
	return err.Last
}

// RetryRemoteCall invokes call at most the policy's allowed number of times.
// Every attempt receives the same idempotency and fencing identity with an
// incremented Attempt number. Context cancellation stops both calls and
// backoff waits immediately.
func RetryRemoteCall[T any](ctx context.Context, call func(context.Context, RemoteCallRequest) (T, error), policy RemoteCallPolicy) (T, error) {
	var zero T
	if ctx == nil {
		return zero, ErrRemoteCallContextNil
	}
	if call == nil {
		return zero, ErrRemoteCallFunctionNil
	}
	maxAttempts, err := normalizeRemoteCallPolicy(&policy)
	if err != nil {
		return zero, err
	}
	shouldRetry := policy.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = defaultRemoteCallRetryPredicate
	}

	request := RemoteCallRequest{
		Method:         policy.Method,
		IdempotencyKey: policy.IdempotencyKey,
		FencingToken:   policy.FencingToken,
	}
	backoff := policy.InitialBackoff
	var lastError error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		request.Attempt = attempt
		value, err := call(ctx, request)
		if err == nil {
			return value, nil
		}
		lastError = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
		if attempt == maxAttempts || !shouldRetry(err) {
			if attempt == maxAttempts {
				return zero, RemoteCallError{Attempts: attempt, Last: lastError}
			}
			return zero, err
		}

		delay := backoff
		if policy.Jitter != nil {
			delay = policy.Jitter(attempt, delay)
			if delay < 0 {
				delay = 0
			}
			if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
				delay = policy.MaxBackoff
			}
		}
		if policy.OnRetry != nil {
			policy.OnRetry(RemoteCallRetryEvent{
				Method:       policy.Method,
				Attempt:      attempt,
				NextAttempt:  attempt + 1,
				Delay:        delay,
				FencingToken: policy.FencingToken,
			})
		}
		if err := waitRemoteCall(ctx, delay); err != nil {
			return zero, err
		}
		backoff = nextRemoteCallBackoff(backoff, policy.MaxBackoff)
	}
	return zero, RemoteCallError{Attempts: maxAttempts, Last: lastError}
}

func normalizeRemoteCallPolicy(policy *RemoteCallPolicy) (int, error) {
	if policy == nil {
		return 0, ErrRemoteCallPolicyInvalid
	}
	switch policy.Method {
	case RemoteCallRead:
	case RemoteCallIdempotentWrite:
		policy.IdempotencyKey = strings.TrimSpace(policy.IdempotencyKey)
		if policy.IdempotencyKey == "" || policy.FencingToken == 0 {
			return 0, ErrRemoteCallPolicyInvalid
		}
	case RemoteCallNonIdempotent:
	default:
		return 0, ErrRemoteCallPolicyInvalid
	}
	if policy.MaxAttempts < 0 || policy.MaxAttempts > maxRemoteCallAttempts || policy.InitialBackoff < 0 || policy.MaxBackoff < 0 || policy.MaxBackoff > maxRemoteCallBackoff {
		return 0, ErrRemoteCallPolicyInvalid
	}
	if policy.MaxBackoff > 0 && policy.MaxBackoff < policy.InitialBackoff {
		return 0, ErrRemoteCallPolicyInvalid
	}
	maxAttempts := policy.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultRemoteCallMaxAttempts
		if policy.Method == RemoteCallNonIdempotent {
			maxAttempts = 1
		}
	}
	if policy.Method == RemoteCallNonIdempotent && maxAttempts > 1 {
		return 0, ErrRemoteCallNonIdempotent
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = policy.InitialBackoff
	}
	return maxAttempts, nil
}

func defaultRemoteCallRetryPredicate(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func waitRemoteCall(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRemoteCallBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 || maximum <= 0 {
		return 0
	}
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}
