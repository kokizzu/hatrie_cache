package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestTU52AdaptiveBreakerAdaptsCooldownThresholdAndFailureClass(t *testing.T) {
	config := AdaptiveCircuitBreakerConfig{
		Enabled:           true,
		MinFailures:       2,
		MaxFailures:       4,
		MinCooldown:       time.Minute,
		MaxCooldown:       5 * time.Minute,
		ThresholdStep:     1,
		CooldownStep:      10 * time.Second,
		RecoverySuccesses: 2,
	}
	now := time.Unix(100, 0)
	snapshot, err := NewAdaptiveCircuitBreakerSnapshot(config)
	if err != nil {
		t.Fatalf("NewAdaptiveCircuitBreakerSnapshot() error = %v", err)
	}
	if snapshot.Threshold != 2 || snapshot.Cooldown != time.Minute {
		t.Fatalf("initial snapshot = %#v, want threshold 2/cooldown 1m", snapshot)
	}

	var transitioned bool
	snapshot, transitioned, err = RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassTransport, "connection reset", now)
	if err != nil || transitioned || snapshot.State != StateClosed || snapshot.Failures != 1 {
		t.Fatalf("first failure = %#v/%v/%v", snapshot, transitioned, err)
	}
	snapshot, transitioned, err = RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassTransport, "connection reset", now.Add(time.Second))
	if err != nil || !transitioned || snapshot.State != StateOpen || snapshot.Cooldown != 70*time.Second || snapshot.OpenEvents != 1 {
		t.Fatalf("opening failure = %#v/%v/%v", snapshot, transitioned, err)
	}
	if snapshot.LastFailureClass != FailureClassTransport {
		t.Fatalf("LastFailureClass = %q, want %q", snapshot.LastFailureClass, FailureClassTransport)
	}

	decision, err := BeforeAdaptiveAttempt(snapshot, config, now.Add(time.Minute))
	if err != nil || decision.Allowed || decision.State != StateOpen {
		t.Fatalf("before cooldown decision = %#v/%v", decision, err)
	}
	decision, err = BeforeAdaptiveAttempt(snapshot, config, now.Add(71*time.Second))
	if err != nil || !decision.Allowed || decision.State != StateHalfOpen {
		t.Fatalf("after cooldown decision = %#v/%v", decision, err)
	}
	snapshot.State = decision.State
	snapshot, transitioned, err = RecordAdaptiveSuccess(snapshot, config, now.Add(71*time.Second))
	if err != nil || !transitioned || snapshot.State != StateClosed || snapshot.ConsecutiveRecoveries != 1 {
		t.Fatalf("first recovery = %#v/%v/%v", snapshot, transitioned, err)
	}

	snapshot, _, err = RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassProtocol, "protocol mismatch", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("protocol first failure = %v", err)
	}
	snapshot, transitioned, err = RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassProtocol, "protocol mismatch", now.Add(2*time.Minute+time.Second))
	if err != nil || !transitioned || snapshot.Cooldown != 100*time.Second || snapshot.OpenEvents != 2 {
		t.Fatalf("protocol opening failure = %#v/%v/%v", snapshot, transitioned, err)
	}
	decision, err = BeforeAdaptiveAttempt(snapshot, config, now.Add(2*time.Minute+101*time.Second))
	if err != nil || !decision.Allowed || decision.State != StateHalfOpen {
		t.Fatalf("second half-open decision = %#v/%v", decision, err)
	}
	snapshot.State = decision.State
	snapshot, transitioned, err = RecordAdaptiveSuccess(snapshot, config, now.Add(2*time.Minute+101*time.Second))
	if err != nil || !transitioned || snapshot.Threshold != 3 || snapshot.Cooldown != 90*time.Second || snapshot.ConsecutiveRecoveries != 2 {
		t.Fatalf("second recovery = %#v/%v/%v", snapshot, transitioned, err)
	}
}

func TestTU52AdaptiveBreakerDisabledAndInvalidInputs(t *testing.T) {
	var disabled AdaptiveCircuitBreakerConfig
	if disabled.Enabled {
		t.Fatal("zero-value adaptive config enabled")
	}
	snapshot, err := NewAdaptiveCircuitBreakerSnapshot(disabled)
	if err != nil || snapshot != (AdaptiveCircuitBreakerSnapshot{}) {
		t.Fatalf("disabled snapshot = %#v/%v, want zero", snapshot, err)
	}
	decision, err := BeforeAdaptiveAttempt(snapshot, disabled, time.Unix(1, 0))
	if err != nil || !decision.Allowed || decision.State != StateClosed {
		t.Fatalf("disabled decision = %#v/%v", decision, err)
	}

	invalidConfigs := []AdaptiveCircuitBreakerConfig{
		{Enabled: true, MinFailures: -1},
		{Enabled: true, MinFailures: 3, MaxFailures: 2},
		{Enabled: true, MinCooldown: time.Second, MaxCooldown: time.Millisecond},
		{Enabled: true, RecoverySuccesses: -1},
	}
	for _, invalid := range invalidConfigs {
		if _, err := NewAdaptiveCircuitBreakerSnapshot(invalid); !errors.Is(err, ErrAdaptiveCircuitBreakerConfigInvalid) {
			t.Fatalf("invalid config %#v error = %v, want ErrAdaptiveCircuitBreakerConfigInvalid", invalid, err)
		}
	}

	config := AdaptiveCircuitBreakerConfig{Enabled: true}
	snapshot, err = NewAdaptiveCircuitBreakerSnapshot(config)
	if err != nil {
		t.Fatalf("default config error = %v", err)
	}
	before := snapshot
	if _, _, err := RecordAdaptiveFailure(snapshot, config, StateClosed, CircuitFailureClass("bad"), "bad", time.Unix(1, 0)); !errors.Is(err, ErrAdaptiveCircuitBreakerFailureClassInvalid) {
		t.Fatalf("invalid class error = %v, want ErrAdaptiveCircuitBreakerFailureClassInvalid", err)
	}
	if snapshot != before {
		t.Fatalf("invalid class mutated source snapshot: %#v -> %#v", before, snapshot)
	}
	if _, _, err := RecordAdaptiveFailure(snapshot, config, StateOpen, FailureClassTransport, "bad state", time.Unix(1, 0)); !errors.Is(err, ErrAdaptiveCircuitBreakerAttemptStateInvalid) {
		t.Fatalf("invalid attempt state error = %v, want ErrAdaptiveCircuitBreakerAttemptStateInvalid", err)
	}
}

func TestTU52AdaptiveBreakerBoundsAndRecovery(t *testing.T) {
	config := AdaptiveCircuitBreakerConfig{
		Enabled:           true,
		MinFailures:       1,
		MaxFailures:       2,
		MinCooldown:       time.Second,
		MaxCooldown:       3 * time.Second,
		ThresholdStep:     1,
		CooldownStep:      time.Second,
		RecoverySuccesses: 1,
	}
	now := time.Unix(10, 0)
	snapshot, err := NewAdaptiveCircuitBreakerSnapshot(config)
	if err != nil {
		t.Fatalf("NewAdaptiveCircuitBreakerSnapshot() error = %v", err)
	}
	for failure := 0; failure < 5; failure++ {
		attemptTime := now.Add(time.Duration(failure) * 10 * time.Second)
		if snapshot.State == StateOpen {
			decision, decisionErr := BeforeAdaptiveAttempt(snapshot, config, attemptTime)
			if decisionErr != nil || !decision.Allowed || decision.State != StateHalfOpen {
				t.Fatalf("half-open decision = %#v/%v", decision, decisionErr)
			}
			snapshot.State = decision.State
			var transitioned bool
			snapshot, transitioned, err = RecordAdaptiveFailure(snapshot, config, StateHalfOpen, FailureClassOverload, "overload", attemptTime)
			if err != nil || !transitioned {
				t.Fatalf("half-open failure = %#v/%v/%v", snapshot, transitioned, err)
			}
			continue
		}
		var transitioned bool
		snapshot, transitioned, err = RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassOverload, "overload", attemptTime)
		if err != nil || !transitioned {
			t.Fatalf("bounded failure = %#v/%v/%v", snapshot, transitioned, err)
		}
	}
	if snapshot.Cooldown != config.MaxCooldown || snapshot.Threshold != config.MinFailures {
		t.Fatalf("bounded snapshot = %#v, want cooldown %s and threshold %d", snapshot, config.MaxCooldown, config.MinFailures)
	}
}
