package hatReplication_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func TestCircuitBreakerOpensThenHalfOpenThenCloses(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	config := hatReplication.CircuitBreakerConfig{Failures: 2, Cooldown: time.Minute}
	state, transition := hatReplication.RecordFailure(hatReplication.CircuitBreakerSnapshot{}, config, hatReplication.StateClosed, "first", now)
	if transition || state.Failures != 1 || state.State != hatReplication.StateClosed {
		t.Fatalf("first failure = %#v transitioned=%v", state, transition)
	}
	state, transition = hatReplication.RecordFailure(state, config, hatReplication.StateClosed, "second", now)
	if !transition || state.State != hatReplication.StateOpen || state.OpenUntil == nil {
		t.Fatalf("second failure = %#v transitioned=%v", state, transition)
	}
	decision := hatReplication.BeforeAttempt(state, config, now.Add(30*time.Second))
	if decision.Allowed || decision.State != hatReplication.StateOpen {
		t.Fatalf("open decision = %#v", decision)
	}
	decision = hatReplication.BeforeAttempt(state, config, now.Add(time.Minute))
	if !decision.Allowed || decision.State != hatReplication.StateHalfOpen {
		t.Fatalf("half-open decision = %#v", decision)
	}
	state.State = decision.State
	state, transition = hatReplication.RecordSuccess(state, now.Add(time.Minute))
	if !transition || state.State != hatReplication.StateClosed || state.Failures != 0 {
		t.Fatalf("success = %#v transitioned=%v", state, transition)
	}
}
