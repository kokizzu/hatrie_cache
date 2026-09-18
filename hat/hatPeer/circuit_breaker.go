package hatPeer

import (
	"context"
	"errors"
	"sync"
	"time"

	"hatrie_cache/hat/hatReplication"
)

var (
	// ErrCompactPeerCircuitBreakerNil reports use of a nil breaker.
	ErrCompactPeerCircuitBreakerNil = errors.New("hatPeer: compact peer circuit breaker is nil")
	// ErrCompactPeerCircuitBreakerSessionRequired reports a missing session.
	ErrCompactPeerCircuitBreakerSessionRequired = errors.New("hatPeer: compact peer circuit breaker session is required")
	// ErrCompactPeerCircuitBreakerOptionsInvalid reports invalid breaker limits.
	ErrCompactPeerCircuitBreakerOptionsInvalid = errors.New("hatPeer: compact peer circuit breaker options are invalid")
	// ErrCompactPeerCircuitOpen reports a request rejected without writing to a
	// peer while its circuit is open or a half-open probe is already running.
	ErrCompactPeerCircuitOpen = errors.New("hatPeer: compact peer circuit is open")
)

const (
	// DefaultCompactPeerCircuitBreakerFailures opens a breaker after five
	// consecutive non-context call failures.
	DefaultCompactPeerCircuitBreakerFailures = 5
	// DefaultCompactPeerCircuitBreakerCooldown controls the default half-open
	// retry delay.
	DefaultCompactPeerCircuitBreakerCooldown = 30 * time.Second
)

// CompactPeerCircuitState is the state reported by an opt-in peer breaker.
type CompactPeerCircuitState = hatReplication.CircuitState

const (
	CompactPeerCircuitClosed   = hatReplication.StateClosed
	CompactPeerCircuitOpen     = hatReplication.StateOpen
	CompactPeerCircuitHalfOpen = hatReplication.StateHalfOpen
)

// CompactPeerCircuitBreakerOptions configures an opt-in breaker around one
// CompactPeerSession. Zero limits use sane defaults; the ordinary session API
// remains unchanged when this wrapper is not constructed.
type CompactPeerCircuitBreakerOptions struct {
	Failures int
	Cooldown time.Duration
	Now      func() time.Time
}

// CompactPeerCircuitBreakerStatus is a stable operational snapshot. HealthScore
// is a local admission signal, not a remote service health guarantee.
type CompactPeerCircuitBreakerStatus struct {
	State               CompactPeerCircuitState
	ConsecutiveFailures int
	HealthScore         int
	OpenUntil           *time.Time
	LastFailureReason   string
}

// CompactPeerCircuitBreaker wraps one session with failure admission and
// health reporting. A cooldown permits one half-open probe at a time.
type CompactPeerCircuitBreaker struct {
	session *CompactPeerSession
	config  hatReplication.CircuitBreakerConfig
	now     func() time.Time

	mu            sync.Mutex
	snapshot      hatReplication.CircuitBreakerSnapshot
	probeInFlight bool
}

// NewCompactPeerCircuitBreaker creates an opt-in breaker for session calls.
func NewCompactPeerCircuitBreaker(session *CompactPeerSession, options CompactPeerCircuitBreakerOptions) (*CompactPeerCircuitBreaker, error) {
	if session == nil {
		return nil, ErrCompactPeerCircuitBreakerSessionRequired
	}
	if options.Failures < 0 || options.Cooldown < 0 {
		return nil, ErrCompactPeerCircuitBreakerOptionsInvalid
	}
	if options.Failures == 0 {
		options.Failures = DefaultCompactPeerCircuitBreakerFailures
	}
	if options.Cooldown == 0 {
		options.Cooldown = DefaultCompactPeerCircuitBreakerCooldown
	}
	if options.Failures < 1 || options.Cooldown <= 0 {
		return nil, ErrCompactPeerCircuitBreakerOptionsInvalid
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &CompactPeerCircuitBreaker{
		session: session,
		config:  hatReplication.CircuitBreakerConfig{Failures: options.Failures, Cooldown: options.Cooldown},
		now:     options.Now,
		snapshot: hatReplication.CircuitBreakerSnapshot{
			State: hatReplication.StateClosed,
		},
	}, nil
}

// Call executes one request if the circuit admits it.
func (breaker *CompactPeerCircuitBreaker) Call(ctx context.Context, command, payload []byte) (CompactFrame, error) {
	if err := breaker.begin(ctx); err != nil {
		return CompactFrame{}, err
	}
	response, err := breaker.session.Call(ctx, command, payload)
	return breaker.finish(response, err)
}

// CallTemplate executes one prepared request if the circuit admits it.
func (breaker *CompactPeerCircuitBreaker) CallTemplate(ctx context.Context, template CompactRequestTemplate, payload []byte) (CompactFrame, error) {
	if err := breaker.begin(ctx); err != nil {
		return CompactFrame{}, err
	}
	response, err := breaker.session.CallTemplate(ctx, template, payload)
	return breaker.finish(response, err)
}

// Status returns the current breaker state and bounded health score.
func (breaker *CompactPeerCircuitBreaker) Status() CompactPeerCircuitBreakerStatus {
	if breaker == nil {
		return CompactPeerCircuitBreakerStatus{State: CompactPeerCircuitClosed, HealthScore: 100}
	}
	breaker.mu.Lock()
	snapshot := cloneCompactPeerCircuitSnapshot(breaker.snapshot)
	config := breaker.config
	breaker.mu.Unlock()
	return compactPeerCircuitStatus(snapshot, config)
}

func (breaker *CompactPeerCircuitBreaker) begin(ctx context.Context) error {
	if breaker == nil {
		return ErrCompactPeerCircuitBreakerNil
	}
	if breaker.session == nil {
		return ErrCompactPeerCircuitBreakerSessionRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := breaker.admit(); err != nil {
		return err
	}
	return nil
}

func (breaker *CompactPeerCircuitBreaker) finish(response CompactFrame, err error) (CompactFrame, error) {
	if err == nil {
		breaker.recordSuccess()
		return response, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		breaker.releaseProbe()
		return CompactFrame{}, err
	}
	breaker.recordFailure(err)
	return CompactFrame{}, err
}

func (breaker *CompactPeerCircuitBreaker) admit() error {
	now := breaker.now()
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	decision := hatReplication.BeforeAttempt(breaker.snapshot, breaker.config, now)
	if !decision.Allowed {
		return ErrCompactPeerCircuitOpen
	}
	if decision.State == hatReplication.StateHalfOpen {
		if breaker.probeInFlight {
			return ErrCompactPeerCircuitOpen
		}
		breaker.snapshot.State = hatReplication.StateHalfOpen
		breaker.probeInFlight = true
	}
	return nil
}

func (breaker *CompactPeerCircuitBreaker) recordSuccess() {
	breaker.mu.Lock()
	breaker.snapshot, _ = hatReplication.RecordSuccess(breaker.snapshot, breaker.now())
	breaker.probeInFlight = false
	breaker.mu.Unlock()
}

func (breaker *CompactPeerCircuitBreaker) recordFailure(err error) {
	breaker.mu.Lock()
	state := breaker.snapshot.State
	if state == "" {
		state = hatReplication.StateClosed
	}
	breaker.snapshot, _ = hatReplication.RecordFailure(breaker.snapshot, breaker.config, state, err.Error(), breaker.now())
	breaker.probeInFlight = false
	breaker.mu.Unlock()
}

func (breaker *CompactPeerCircuitBreaker) releaseProbe() {
	breaker.mu.Lock()
	breaker.probeInFlight = false
	breaker.mu.Unlock()
}

func compactPeerCircuitStatus(snapshot hatReplication.CircuitBreakerSnapshot, config hatReplication.CircuitBreakerConfig) CompactPeerCircuitBreakerStatus {
	status := CompactPeerCircuitBreakerStatus{
		State:               snapshot.State,
		ConsecutiveFailures: snapshot.Failures,
		LastFailureReason:   snapshot.LastFailureReason,
		OpenUntil:           cloneCompactPeerTime(snapshot.OpenUntil),
		HealthScore:         100,
	}
	switch snapshot.State {
	case hatReplication.StateOpen:
		status.HealthScore = 0
	case hatReplication.StateHalfOpen:
		status.HealthScore = 50
	case hatReplication.StateClosed:
		if config.Failures > 0 && snapshot.Failures > 0 {
			status.HealthScore = 100 - snapshot.Failures*100/config.Failures
			if status.HealthScore < 1 {
				status.HealthScore = 1
			}
		}
	}
	return status
}

func cloneCompactPeerCircuitSnapshot(snapshot hatReplication.CircuitBreakerSnapshot) hatReplication.CircuitBreakerSnapshot {
	snapshot.OpenedAt = cloneCompactPeerTime(snapshot.OpenedAt)
	snapshot.OpenUntil = cloneCompactPeerTime(snapshot.OpenUntil)
	snapshot.LastFailureAt = cloneCompactPeerTime(snapshot.LastFailureAt)
	snapshot.LastSuccessAt = cloneCompactPeerTime(snapshot.LastSuccessAt)
	return snapshot
}

func cloneCompactPeerTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
