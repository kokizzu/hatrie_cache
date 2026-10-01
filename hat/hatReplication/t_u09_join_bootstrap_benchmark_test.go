package hatReplication

import "testing"

var joinBootstrapBenchmarkState JoinBootstrapState

func BenchmarkJoinBootstrapBaselineDirect(b *testing.B) {
	type directState struct {
		applied uint64
		source  uint64
		phase   JoinBootstrapPhase
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		state := directState{applied: 100, phase: JoinBootstrapCatchingUp}
		state.applied = uint64(100 + i%1000)
		state.source = state.applied
		state.phase = JoinBootstrapReady
		state.phase = JoinBootstrapActivated
		joinBootstrapBenchmarkState.AppliedSequence = state.applied
		joinBootstrapBenchmarkState.SourceSequence = state.source
		joinBootstrapBenchmarkState.Phase = state.phase
	}
}

func BenchmarkJoinBootstrapStateTransitions(b *testing.B) {
	state, err := NewJoinBootstrapState("join-benchmark", "leader", "replica", 11, 100)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		current, err := state.BeginCatchUp()
		if err != nil {
			b.Fatal(err)
		}
		current, err = current.RecordApplied(uint64(100 + i%1000))
		if err != nil {
			b.Fatal(err)
		}
		current, err = current.PrepareActivation(11, current.AppliedSequence)
		if err != nil {
			b.Fatal(err)
		}
		current, err = current.Activate(11)
		if err != nil {
			b.Fatal(err)
		}
		joinBootstrapBenchmarkState = current
	}
}
