package hatReplication

import (
	"testing"
	"time"
)

var tu52BenchmarkSink bool

func BenchmarkTU52StaticBeforeAttempt(b *testing.B) {
	config := CircuitBreakerConfig{Failures: 5, Cooldown: time.Minute}
	snapshot := CircuitBreakerSnapshot{}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		tu52BenchmarkSink = BeforeAttempt(snapshot, config, now).Allowed
	}
}

func BenchmarkTU52AdaptiveBeforeAttempt(b *testing.B) {
	config := AdaptiveCircuitBreakerConfig{Enabled: true}
	snapshot, err := NewAdaptiveCircuitBreakerSnapshot(config)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		decision, decisionErr := BeforeAdaptiveAttempt(snapshot, config, now)
		if decisionErr != nil {
			b.Fatal(decisionErr)
		}
		tu52BenchmarkSink = decision.Allowed
	}
}

func BenchmarkTU52AdaptiveFailure(b *testing.B) {
	config := AdaptiveCircuitBreakerConfig{Enabled: true}
	snapshot, err := NewAdaptiveCircuitBreakerSnapshot(config)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		next, _, failureErr := RecordAdaptiveFailure(snapshot, config, StateClosed, FailureClassTransport, "timeout", now)
		if failureErr != nil {
			b.Fatal(failureErr)
		}
		snapshot = next
		if snapshot.State == StateOpen {
			snapshot.State = StateClosed
			snapshot.Failures = 0
		}
		tu52BenchmarkSink = snapshot.State == StateOpen
	}
}

func BenchmarkTU52StaticFailure(b *testing.B) {
	config := CircuitBreakerConfig{Failures: 3, Cooldown: time.Minute}
	snapshot := CircuitBreakerSnapshot{}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		next, _ := RecordFailure(snapshot, config, StateClosed, "timeout", now)
		snapshot = next
		if snapshot.State == StateOpen {
			snapshot.State = StateClosed
			snapshot.Failures = 0
		}
		tu52BenchmarkSink = snapshot.State == StateOpen
	}
}
