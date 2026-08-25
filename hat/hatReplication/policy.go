// Package hatReplication provides transport-independent replication policy
// components. It does not depend on the cache server or replication wire.
package hatReplication

import "time"

// CircuitState is the externally visible circuit-breaker state.
type CircuitState string

const (
	StateClosed   CircuitState = "closed"
	StateOpen     CircuitState = "open"
	StateHalfOpen CircuitState = "half_open"
)

// CircuitBreakerConfig controls failure and cooldown thresholds. A zero or
// negative value disables breaker transitions.
type CircuitBreakerConfig struct {
	Failures int
	Cooldown time.Duration
}

// Enabled reports whether config enables circuit breaking.
func (config CircuitBreakerConfig) Enabled() bool {
	return config.Failures > 0 && config.Cooldown > 0
}

// CircuitBreakerSnapshot contains a transport-independent breaker state.
type CircuitBreakerSnapshot struct {
	State             CircuitState
	Failures          int
	OpenedAt          *time.Time
	OpenUntil         *time.Time
	LastFailureAt     *time.Time
	LastSuccessAt     *time.Time
	LastFailureReason string
}

// AttemptDecision describes whether a target may receive one request.
type AttemptDecision struct {
	Allowed bool
	State   CircuitState
}

// BeforeAttempt computes the next admission decision without mutating state.
func BeforeAttempt(snapshot CircuitBreakerSnapshot, config CircuitBreakerConfig, now time.Time) AttemptDecision {
	if !config.Enabled() {
		return AttemptDecision{Allowed: true, State: StateClosed}
	}
	switch snapshot.State {
	case StateOpen:
		if snapshot.OpenUntil == nil || now.Before(*snapshot.OpenUntil) {
			return AttemptDecision{State: StateOpen}
		}
		return AttemptDecision{Allowed: true, State: StateHalfOpen}
	case StateHalfOpen:
		return AttemptDecision{State: StateHalfOpen}
	default:
		return AttemptDecision{Allowed: true, State: StateClosed}
	}
}

// RecordSuccess closes snapshot and reports whether the state transitioned.
func RecordSuccess(snapshot CircuitBreakerSnapshot, now time.Time) (CircuitBreakerSnapshot, bool) {
	previous := snapshot.State
	snapshot.State = StateClosed
	snapshot.Failures = 0
	snapshot.OpenedAt = nil
	snapshot.OpenUntil = nil
	snapshot.LastSuccessAt = cloneTime(now)
	snapshot.LastFailureReason = ""
	return snapshot, previous == StateOpen || previous == StateHalfOpen
}

// RecordFailure increments failures and opens after the configured threshold,
// or immediately after a failed half-open probe.
func RecordFailure(snapshot CircuitBreakerSnapshot, config CircuitBreakerConfig, attemptState CircuitState, reason string, now time.Time) (CircuitBreakerSnapshot, bool) {
	previous := snapshot.State
	snapshot.Failures++
	snapshot.LastFailureAt = cloneTime(now)
	snapshot.LastFailureReason = reason
	if config.Enabled() && (attemptState == StateHalfOpen || snapshot.Failures >= config.Failures) {
		until := now.Add(config.Cooldown)
		snapshot.State = StateOpen
		snapshot.OpenedAt = cloneTime(now)
		snapshot.OpenUntil = cloneTime(until)
		return snapshot, previous != StateOpen
	}
	snapshot.State = StateClosed
	return snapshot, false
}

func cloneTime(value time.Time) *time.Time { return &value }
