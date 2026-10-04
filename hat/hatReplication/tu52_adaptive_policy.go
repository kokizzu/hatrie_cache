package hatReplication

import (
	"errors"
	"time"
)

var (
	// ErrAdaptiveCircuitBreakerConfigInvalid reports invalid adaptive bounds.
	ErrAdaptiveCircuitBreakerConfigInvalid = errors.New("hatReplication: adaptive circuit breaker config is invalid")
	// ErrAdaptiveCircuitBreakerFailureClassInvalid reports an unknown failure class.
	ErrAdaptiveCircuitBreakerFailureClassInvalid = errors.New("hatReplication: adaptive circuit breaker failure class is invalid")
	// ErrAdaptiveCircuitBreakerAttemptStateInvalid reports an impossible failure state.
	ErrAdaptiveCircuitBreakerAttemptStateInvalid = errors.New("hatReplication: adaptive circuit breaker attempt state is invalid")
)

const (
	DefaultAdaptiveCircuitBreakerMinFailures       = 3
	DefaultAdaptiveCircuitBreakerMaxFailures       = 10
	DefaultAdaptiveCircuitBreakerMinCooldown       = 5 * time.Second
	DefaultAdaptiveCircuitBreakerMaxCooldown       = 5 * time.Minute
	DefaultAdaptiveCircuitBreakerThresholdStep     = 1
	DefaultAdaptiveCircuitBreakerCooldownStep      = 5 * time.Second
	DefaultAdaptiveCircuitBreakerRecoverySuccesses = 2
)

// CircuitFailureClass lets an adaptive breaker apply a bounded response to a
// peer-specific failure family without retaining the raw error.
type CircuitFailureClass string

const (
	FailureClassUnknown   CircuitFailureClass = "unknown"
	FailureClassTransport CircuitFailureClass = "transport"
	FailureClassTimeout   CircuitFailureClass = "timeout"
	FailureClassOverload  CircuitFailureClass = "overload"
	FailureClassProtocol  CircuitFailureClass = "protocol"
)

// AdaptiveCircuitBreakerConfig enables bounded adaptation. The zero value is
// disabled; existing fixed-threshold breaker paths remain unchanged.
type AdaptiveCircuitBreakerConfig struct {
	Enabled           bool
	MinFailures       int
	MaxFailures       int
	MinCooldown       time.Duration
	MaxCooldown       time.Duration
	ThresholdStep     int
	CooldownStep      time.Duration
	RecoverySuccesses int
}

// AdaptiveCircuitBreakerSnapshot is the caller-owned state for one peer.
// It embeds the transport-independent fixed-breaker state for easy handoff.
type AdaptiveCircuitBreakerSnapshot struct {
	CircuitBreakerSnapshot
	Threshold             int
	Cooldown              time.Duration
	ConsecutiveRecoveries int
	OpenEvents            uint64
	LastFailureClass      CircuitFailureClass
}

// NewAdaptiveCircuitBreakerSnapshot initializes one bounded adaptive state.
func NewAdaptiveCircuitBreakerSnapshot(config AdaptiveCircuitBreakerConfig) (AdaptiveCircuitBreakerSnapshot, error) {
	normalized, err := config.normalized()
	if err != nil {
		return AdaptiveCircuitBreakerSnapshot{}, err
	}
	if !normalized.Enabled {
		return AdaptiveCircuitBreakerSnapshot{}, nil
	}
	return AdaptiveCircuitBreakerSnapshot{
		Threshold: normalized.MinFailures,
		Cooldown:  normalized.MinCooldown,
	}, nil
}

// BeforeAdaptiveAttempt decides whether one adaptive peer attempt is allowed.
func BeforeAdaptiveAttempt(snapshot AdaptiveCircuitBreakerSnapshot, config AdaptiveCircuitBreakerConfig, now time.Time) (AttemptDecision, error) {
	normalized, err := config.normalized()
	if err != nil {
		return AttemptDecision{}, err
	}
	if !normalized.Enabled {
		return AttemptDecision{Allowed: true, State: StateClosed}, nil
	}
	state := adaptiveState(snapshot.State)
	switch state {
	case StateOpen:
		if snapshot.OpenUntil == nil || now.Before(*snapshot.OpenUntil) {
			return AttemptDecision{State: StateOpen}, nil
		}
		return AttemptDecision{Allowed: true, State: StateHalfOpen}, nil
	case StateHalfOpen:
		return AttemptDecision{Allowed: true, State: StateHalfOpen}, nil
	default:
		return AttemptDecision{Allowed: true, State: StateClosed}, nil
	}
}

// RecordAdaptiveSuccess closes the breaker. A configured number of successful
// half-open recoveries raises the failure threshold and relaxes cooldown.
func RecordAdaptiveSuccess(snapshot AdaptiveCircuitBreakerSnapshot, config AdaptiveCircuitBreakerConfig, now time.Time) (AdaptiveCircuitBreakerSnapshot, bool, error) {
	normalized, err := config.normalized()
	if err != nil {
		return snapshot, false, err
	}
	if !normalized.Enabled {
		return snapshot, false, nil
	}
	previous := adaptiveState(snapshot.State)
	snapshot.Threshold, snapshot.Cooldown = adaptiveBounds(snapshot, normalized)
	snapshot.State = StateClosed
	snapshot.Failures = 0
	snapshot.OpenedAt = nil
	snapshot.OpenUntil = nil
	snapshot.LastSuccessAt = cloneTime(now)
	snapshot.LastFailureReason = ""
	if previous == StateHalfOpen {
		snapshot.ConsecutiveRecoveries++
		if snapshot.ConsecutiveRecoveries >= normalized.RecoverySuccesses {
			snapshot.Threshold = minInt(normalized.MaxFailures, snapshot.Threshold+normalized.ThresholdStep)
			snapshot.Cooldown = maxDuration(normalized.MinCooldown, snapshot.Cooldown-normalized.CooldownStep)
		}
	} else {
		snapshot.ConsecutiveRecoveries = 0
	}
	return snapshot, previous == StateOpen || previous == StateHalfOpen, nil
}

// RecordAdaptiveFailure records one peer failure. Repeated opens increase the
// cooldown by a class-weighted bounded step; the threshold changes only after
// observed half-open recovery succeeds.
func RecordAdaptiveFailure(snapshot AdaptiveCircuitBreakerSnapshot, config AdaptiveCircuitBreakerConfig, attemptState CircuitState, class CircuitFailureClass, reason string, now time.Time) (AdaptiveCircuitBreakerSnapshot, bool, error) {
	normalized, err := config.normalized()
	if err != nil {
		return snapshot, false, err
	}
	if !normalized.Enabled {
		return snapshot, false, nil
	}
	if attemptState != StateClosed && attemptState != StateHalfOpen {
		return snapshot, false, ErrAdaptiveCircuitBreakerAttemptStateInvalid
	}
	class, err = normalizeCircuitFailureClass(class)
	if err != nil {
		return snapshot, false, err
	}
	previous := adaptiveState(snapshot.State)
	snapshot.Threshold, snapshot.Cooldown = adaptiveBounds(snapshot, normalized)
	snapshot.Failures++
	snapshot.LastFailureAt = cloneTime(now)
	snapshot.LastFailureReason = reason
	snapshot.LastFailureClass = class
	if attemptState == StateHalfOpen {
		snapshot.ConsecutiveRecoveries = 0
	}
	if attemptState == StateHalfOpen || snapshot.Failures >= snapshot.Threshold {
		wasOpen := previous == StateOpen
		snapshot.State = StateOpen
		snapshot.OpenedAt = cloneTime(now)
		snapshot.Cooldown = addAdaptiveCooldown(snapshot.Cooldown, normalized.CooldownStep, normalized.MaxCooldown, circuitFailureSeverity(class))
		snapshot.OpenUntil = cloneTime(now.Add(snapshot.Cooldown))
		if !wasOpen {
			snapshot.OpenEvents++
		}
		return snapshot, !wasOpen, nil
	}
	snapshot.State = StateClosed
	return snapshot, false, nil
}

func (config AdaptiveCircuitBreakerConfig) normalized() (AdaptiveCircuitBreakerConfig, error) {
	if !config.Enabled {
		return config, nil
	}
	if config.MinFailures == 0 {
		config.MinFailures = DefaultAdaptiveCircuitBreakerMinFailures
	}
	if config.MaxFailures == 0 {
		config.MaxFailures = DefaultAdaptiveCircuitBreakerMaxFailures
	}
	if config.MinCooldown == 0 {
		config.MinCooldown = DefaultAdaptiveCircuitBreakerMinCooldown
	}
	if config.MaxCooldown == 0 {
		config.MaxCooldown = DefaultAdaptiveCircuitBreakerMaxCooldown
	}
	if config.ThresholdStep == 0 {
		config.ThresholdStep = DefaultAdaptiveCircuitBreakerThresholdStep
	}
	if config.CooldownStep == 0 {
		config.CooldownStep = DefaultAdaptiveCircuitBreakerCooldownStep
	}
	if config.RecoverySuccesses == 0 {
		config.RecoverySuccesses = DefaultAdaptiveCircuitBreakerRecoverySuccesses
	}
	if config.MinFailures < 1 || config.MaxFailures < config.MinFailures || config.MinFailures > 1_000_000 || config.MaxFailures > 1_000_000 || config.MinCooldown <= 0 || config.MaxCooldown < config.MinCooldown || config.ThresholdStep < 1 || config.ThresholdStep > config.MaxFailures || config.CooldownStep <= 0 || config.CooldownStep > config.MaxCooldown || config.RecoverySuccesses < 1 || config.RecoverySuccesses > 1_000_000 {
		return AdaptiveCircuitBreakerConfig{}, ErrAdaptiveCircuitBreakerConfigInvalid
	}
	return config, nil
}

func adaptiveState(state CircuitState) CircuitState {
	if state == "" {
		return StateClosed
	}
	return state
}

func adaptiveBounds(snapshot AdaptiveCircuitBreakerSnapshot, config AdaptiveCircuitBreakerConfig) (int, time.Duration) {
	threshold := snapshot.Threshold
	if threshold < config.MinFailures || threshold > config.MaxFailures {
		threshold = config.MinFailures
	}
	cooldown := snapshot.Cooldown
	if cooldown < config.MinCooldown || cooldown > config.MaxCooldown {
		cooldown = config.MinCooldown
	}
	return threshold, cooldown
}

func normalizeCircuitFailureClass(class CircuitFailureClass) (CircuitFailureClass, error) {
	if class == "" {
		return FailureClassUnknown, nil
	}
	switch class {
	case FailureClassUnknown, FailureClassTransport, FailureClassTimeout, FailureClassOverload, FailureClassProtocol:
		return class, nil
	default:
		return "", ErrAdaptiveCircuitBreakerFailureClassInvalid
	}
}

func circuitFailureSeverity(class CircuitFailureClass) int {
	switch class {
	case FailureClassProtocol:
		return 3
	case FailureClassTimeout, FailureClassOverload:
		return 2
	default:
		return 1
	}
}

func addAdaptiveCooldown(current, step, maximum time.Duration, severity int) time.Duration {
	if current >= maximum || step <= 0 || severity <= 0 {
		return maximum
	}
	if step > (maximum-current)/time.Duration(severity) {
		return maximum
	}
	return current + step*time.Duration(severity)
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
