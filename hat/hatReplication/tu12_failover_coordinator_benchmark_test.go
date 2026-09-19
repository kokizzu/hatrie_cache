package hatReplication

import "testing"

var tu12BaselineFailoverEligibleSink bool
var tu12FailoverDecisionSink AutomaticFailoverDecision
var tu12FailoverStateSink AutomaticFailoverState

func BenchmarkTU12BaselineFailoverEligibility(b *testing.B) {
	sourceHealthy := false
	candidateHealthy := true
	sourceSequence := uint64(100)
	candidateSequence := uint64(100)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu12BaselineFailoverEligibleSink = !sourceHealthy && candidateHealthy && candidateSequence >= sourceSequence
	}
}

func BenchmarkTU12AutomaticFailoverEvaluate(b *testing.B) {
	options := AutomaticFailoverOptions{Enabled: true, MaxCandidates: DefaultAutomaticFailoverMaxCandidates}
	observation := validAutomaticFailoverObservation()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision, err := evaluateAutomaticFailover(options, observation)
		if err != nil {
			b.Fatal(err)
		}
		tu12FailoverDecisionSink = decision
	}
}

func BenchmarkTU12AutomaticFailoverSnapshot(b *testing.B) {
	coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Propose(validAutomaticFailoverObservation()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu12FailoverStateSink = coordinator.Snapshot()
	}
}
