package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultConnectorHealthMaxAttempts bounds the number of start attempts
	// made by StartWithHealthPolicy when the caller leaves it unset.
	DefaultConnectorHealthMaxAttempts = 3
	// DefaultConnectorHealthInitialBackoff is the first delay between retries.
	DefaultConnectorHealthInitialBackoff = 100 * time.Millisecond
	// DefaultConnectorHealthMaxBackoff caps exponential retry delay.
	DefaultConnectorHealthMaxBackoff = 2 * time.Second
	// MaxConnectorHealthAttempts prevents accidental unbounded retry loops.
	MaxConnectorHealthAttempts = 1024
	// MaxConnectorHealthBackoff prevents an individual policy wait from being
	// configured beyond one hour.
	MaxConnectorHealthBackoff = time.Hour
)

var (
	// ErrConnectorHealthPolicyInvalid reports an invalid retry policy.
	ErrConnectorHealthPolicyInvalid = errors.New("connector health policy is invalid")
	// ErrConnectorHealthQuarantined reports a connector left failed after its
	// retry budget was exhausted.
	ErrConnectorHealthQuarantined = errors.New("connector was quarantined after health retries")
)

// ConnectorHealthPolicy controls caller-driven recovery of a failed connector.
// The registry never starts a background supervisor; callers decide when to
// invoke StartWithHealthPolicy.
type ConnectorHealthPolicy struct {
	// MaxAttempts includes the first start attempt. Zero selects the default.
	MaxAttempts int
	// QuarantineAfter stops retries after this many failed attempts. Zero uses
	// MaxAttempts.
	QuarantineAfter int
	// InitialBackoff is the delay before the second attempt. Zero selects the
	// default.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential backoff. Zero selects the default.
	MaxBackoff time.Duration
}

// ConnectorHealthResult describes one bounded recovery run.
type ConnectorHealthResult struct {
	Attempts    int
	Failures    int
	Recovered   bool
	Quarantined bool
}

// StartWithHealthPolicy starts a connector and retries transient callback
// failures with bounded exponential backoff. Structural registry errors and
// context cancellation are returned immediately. A quarantined connector
// remains in ConnectorFailed until a later caller explicitly retries it.
func (r *ConnectorRegistry) StartWithHealthPolicy(ctx context.Context, id string, policy ConnectorHealthPolicy) (ConnectorHealthResult, error) {
	normalized, err := normalizeConnectorHealthPolicy(policy)
	if err != nil {
		return ConnectorHealthResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	result := ConnectorHealthResult{}
	backoff := normalized.InitialBackoff
	for attempt := 1; attempt <= normalized.MaxAttempts; attempt++ {
		result.Attempts = attempt
		if err := r.Start(ctx, id); err == nil {
			result.Recovered = true
			return result, nil
		} else {
			result.Failures++
			if !retryableConnectorHealthError(err) {
				return result, err
			}
			if attempt >= normalized.QuarantineAfter {
				result.Quarantined = true
				return result, fmt.Errorf("%w: %w", ErrConnectorHealthQuarantined, err)
			}
			if err := waitConnectorHealthBackoff(ctx, backoff); err != nil {
				return result, err
			}
			backoff = nextConnectorHealthBackoff(backoff, normalized.MaxBackoff)
		}
	}

	result.Quarantined = true
	return result, ErrConnectorHealthQuarantined
}

type normalizedConnectorHealthPolicy struct {
	MaxAttempts     int
	QuarantineAfter int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
}

func normalizeConnectorHealthPolicy(policy ConnectorHealthPolicy) (normalizedConnectorHealthPolicy, error) {
	if policy.MaxAttempts < 0 || policy.QuarantineAfter < 0 || policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return normalizedConnectorHealthPolicy{}, ErrConnectorHealthPolicyInvalid
	}
	maxAttempts := policy.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultConnectorHealthMaxAttempts
	}
	if maxAttempts > MaxConnectorHealthAttempts {
		return normalizedConnectorHealthPolicy{}, ErrConnectorHealthPolicyInvalid
	}
	quarantineAfter := policy.QuarantineAfter
	if quarantineAfter == 0 {
		quarantineAfter = maxAttempts
	}
	if quarantineAfter > maxAttempts {
		return normalizedConnectorHealthPolicy{}, ErrConnectorHealthPolicyInvalid
	}
	initialBackoff := policy.InitialBackoff
	if initialBackoff == 0 {
		initialBackoff = DefaultConnectorHealthInitialBackoff
	}
	maxBackoff := policy.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = DefaultConnectorHealthMaxBackoff
	}
	if initialBackoff > MaxConnectorHealthBackoff || maxBackoff > MaxConnectorHealthBackoff || initialBackoff > maxBackoff {
		return normalizedConnectorHealthPolicy{}, ErrConnectorHealthPolicyInvalid
	}
	return normalizedConnectorHealthPolicy{
		MaxAttempts:     maxAttempts,
		QuarantineAfter: quarantineAfter,
		InitialBackoff:  initialBackoff,
		MaxBackoff:      maxBackoff,
	}, nil
}

func retryableConnectorHealthError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch {
	case errors.Is(err, ErrConnectorIDEmpty),
		errors.Is(err, ErrConnectorNotFound),
		errors.Is(err, ErrConnectorInvalidTransition),
		errors.Is(err, ErrConnectorRegistryClosed),
		errors.Is(err, ErrConnectorHealthPolicyInvalid),
		errors.Is(err, ErrConnectorHealthQuarantined):
		return false
	default:
		return true
	}
}

func waitConnectorHealthBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
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

func nextConnectorHealthBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 || current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}
