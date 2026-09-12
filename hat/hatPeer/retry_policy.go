package hatPeer

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

var (
	// ErrRetryPolicyAttemptsInvalid indicates an unsupported attempt bound.
	ErrRetryPolicyAttemptsInvalid = errors.New("hatPeer: retry policy attempt bound is invalid")
	// ErrRetryPolicyBackoffInvalid indicates an invalid backoff range.
	ErrRetryPolicyBackoffInvalid = errors.New("hatPeer: retry policy backoff is invalid")
	// ErrRetryPolicyJitterInvalid indicates an invalid jitter fraction.
	ErrRetryPolicyJitterInvalid = errors.New("hatPeer: retry policy jitter is invalid")
	// ErrRetryPolicyRequestInvalid indicates an unsupported or unsafe request.
	ErrRetryPolicyRequestInvalid = errors.New("hatPeer: retry policy request is invalid")
	// ErrRetryPolicyIdempotencyKeyInvalid indicates an oversized mutation key.
	ErrRetryPolicyIdempotencyKeyInvalid = errors.New("hatPeer: retry policy idempotency key is invalid")
	// ErrRetryPolicyContextRequired indicates a nil execution context.
	ErrRetryPolicyContextRequired = errors.New("hatPeer: retry policy context is required")
	// ErrRetryPolicyCallRequired indicates a nil remote-call function.
	ErrRetryPolicyCallRequired = errors.New("hatPeer: retry policy call is required")
)

const (
	// DefaultRetryPolicyMaxAttempts bounds a normal call and its retries.
	DefaultRetryPolicyMaxAttempts = 3
	// MaxRetryPolicyMaxAttempts prevents accidental retry storms.
	MaxRetryPolicyMaxAttempts = 8
	// MaxRetryPolicyIdempotencyKeyBytes bounds mutation identity metadata.
	MaxRetryPolicyIdempotencyKeyBytes = 256
	// DefaultRetryPolicyInitialBackoff is the first retry delay.
	DefaultRetryPolicyInitialBackoff = 10 * time.Millisecond
	// DefaultRetryPolicyMaxBackoff caps exponential delay.
	DefaultRetryPolicyMaxBackoff = 250 * time.Millisecond
	// DefaultRetryPolicyJitterFraction applies symmetric bounded jitter.
	DefaultRetryPolicyJitterFraction = 0.2
)

// RetryOperation describes the safety class of one remote operation.
type RetryOperation uint8

const (
	RetryInvalid RetryOperation = iota
	// RetryRead is safe to retry when the caller classifies the error as transient.
	RetryRead
	// RetryIdempotentMutation requires an idempotency key and fencing token.
	RetryIdempotentMutation
	// RetryNonIdempotentMutation is always limited to one attempt.
	RetryNonIdempotentMutation
)

// String returns the stable operation name.
func (operation RetryOperation) String() string {
	switch operation {
	case RetryRead:
		return "read"
	case RetryIdempotentMutation:
		return "idempotent_mutation"
	case RetryNonIdempotentMutation:
		return "non_idempotent_mutation"
	default:
		return "invalid"
	}
}

// RetryRequest carries the safety and identity contract for one remote call.
// The idempotency key and fencing token are passed unchanged on every attempt.
type RetryRequest struct {
	Operation      RetryOperation
	IdempotencyKey string
	FencingToken   uint64
}

// RetryAttempt identifies one invocation of a retry-protected call.
type RetryAttempt struct {
	Attempt        uint32
	Operation      RetryOperation
	IdempotencyKey string
	FencingToken   uint64
}

// RetryEvent describes a failed attempt that will be retried.
type RetryEvent struct {
	Request       RetryRequest
	FailedAttempt uint32
	NextAttempt   uint32
	Delay         time.Duration
	Error         error
}

// RetryCall executes one remote attempt. It must pass ctx and the stable
// identity fields to the remote protocol when the operation is a mutation.
type RetryCall func(ctx context.Context, attempt RetryAttempt) error

// RetryableFunc classifies errors that are safe to retry for the request's
// operation class. Transport errors should normally be selected explicitly.
type RetryableFunc func(error) bool

// RetrySleepFunc waits before a retry and must return ctx.Err() when canceled.
// It is injectable so tests and callers with their own scheduler need not use
// real timers.
type RetrySleepFunc func(ctx context.Context, delay time.Duration) error

// RetryObserver receives one event before each retry delay.
type RetryObserver func(event RetryEvent)

// RetryPolicyOptions configures an opt-in method-aware retry policy. Zero
// numeric fields use bounded defaults; set DisableJitter to make delays
// deterministic.
type RetryPolicyOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	JitterFraction float64
	DisableJitter  bool
	Retryable      RetryableFunc
	Sleep          RetrySleepFunc
	Observer       RetryObserver
}

// RetryPolicy executes bounded retries without retaining call state between
// executions. A policy value is safe to copy and use concurrently.
type RetryPolicy struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	jitterFraction float64
	retryable      RetryableFunc
	sleep          RetrySleepFunc
	observer       RetryObserver
}

// NewRetryPolicy validates and creates an opt-in retry policy.
func NewRetryPolicy(options RetryPolicyOptions) (RetryPolicy, error) {
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultRetryPolicyMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > MaxRetryPolicyMaxAttempts {
		return RetryPolicy{}, ErrRetryPolicyAttemptsInvalid
	}
	initialBackoff := options.InitialBackoff
	if initialBackoff == 0 {
		initialBackoff = DefaultRetryPolicyInitialBackoff
	}
	maxBackoff := options.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = DefaultRetryPolicyMaxBackoff
	}
	if initialBackoff < 0 || maxBackoff < 0 || maxBackoff < initialBackoff {
		return RetryPolicy{}, ErrRetryPolicyBackoffInvalid
	}
	if options.JitterFraction < 0 || options.JitterFraction > 1 {
		return RetryPolicy{}, ErrRetryPolicyJitterInvalid
	}
	jitterFraction := options.JitterFraction
	if options.DisableJitter {
		jitterFraction = 0
	} else if jitterFraction == 0 {
		jitterFraction = DefaultRetryPolicyJitterFraction
	}
	retryable := options.Retryable
	if retryable == nil {
		retryable = defaultRetryable
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepRetryPolicy
	}
	return RetryPolicy{
		maxAttempts:    maxAttempts,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		jitterFraction: jitterFraction,
		retryable:      retryable,
		sleep:          sleep,
		observer:       options.Observer,
	}, nil
}

// NewDefaultRetryPolicy creates the bounded default policy with jitter.
func NewDefaultRetryPolicy() RetryPolicy {
	policy, err := NewRetryPolicy(RetryPolicyOptions{})
	if err != nil {
		return RetryPolicy{}
	}
	return policy
}

// Execute runs one request and retries only when its operation class and error
// classifier allow it. Non-idempotent mutations never receive a second call.
func (policy RetryPolicy) Execute(ctx context.Context, request RetryRequest, call RetryCall) error {
	if ctx == nil {
		return ErrRetryPolicyContextRequired
	}
	if call == nil {
		return ErrRetryPolicyCallRequired
	}
	if err := validateRetryRequest(request); err != nil {
		return err
	}
	if policy.maxAttempts == 0 {
		policy = NewDefaultRetryPolicy()
	}
	maxAttempts := policy.maxAttempts
	if request.Operation == RetryNonIdempotentMutation {
		maxAttempts = 1
	}
	for attempt := uint32(1); attempt <= uint32(maxAttempts); attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		metadata := RetryAttempt{
			Attempt:        attempt,
			Operation:      request.Operation,
			IdempotencyKey: request.IdempotencyKey,
			FencingToken:   request.FencingToken,
		}
		err := call(ctx, metadata)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt >= uint32(maxAttempts) || !policy.retryable(err) {
			return err
		}
		delay := policy.retryDelay(attempt)
		if policy.observer != nil {
			policy.observer(RetryEvent{
				Request:       request,
				FailedAttempt: attempt,
				NextAttempt:   attempt + 1,
				Delay:         delay,
				Error:         err,
			})
		}
		if err := policy.sleep(ctx, delay); err != nil {
			return err
		}
	}
	return nil
}

func validateRetryRequest(request RetryRequest) error {
	if len(request.IdempotencyKey) > MaxRetryPolicyIdempotencyKeyBytes {
		return ErrRetryPolicyIdempotencyKeyInvalid
	}
	switch request.Operation {
	case RetryRead, RetryNonIdempotentMutation:
		return nil
	case RetryIdempotentMutation:
		if request.IdempotencyKey == "" || request.FencingToken == 0 {
			return ErrRetryPolicyRequestInvalid
		}
		return nil
	default:
		return ErrRetryPolicyRequestInvalid
	}
}

func (policy RetryPolicy) retryDelay(failedAttempt uint32) time.Duration {
	delay := policy.initialBackoff
	for attempt := uint32(1); attempt < failedAttempt; attempt++ {
		if delay >= policy.maxBackoff-delay {
			delay = policy.maxBackoff
			break
		}
		delay *= 2
	}
	if policy.jitterFraction == 0 || delay == 0 {
		return delay
	}
	factor := 1 + (rand.Float64()*2-1)*policy.jitterFraction
	jittered := time.Duration(float64(delay) * factor)
	if jittered < 0 {
		return 0
	}
	if jittered > policy.maxBackoff {
		return policy.maxBackoff
	}
	return jittered
}

func defaultRetryable(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func sleepRetryPolicy(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
