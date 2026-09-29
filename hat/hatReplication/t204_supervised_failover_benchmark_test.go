package hatReplication

import "testing"

var t204SupervisedFailoverStateSink SupervisedFailoverState

func BenchmarkT204AutomaticFailoverCommitLifecycle(b *testing.B) {
	observation := validAutomaticFailoverObservation()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
		if err != nil {
			b.Fatal(err)
		}
		proposal, err := coordinator.Propose(observation)
		if err != nil {
			b.Fatal(err)
		}
		state, err := coordinator.Commit(proposal)
		if err != nil {
			b.Fatal(err)
		}
		t204SupervisedFailoverStateSink = SupervisedFailoverState{Phase: SupervisedFailoverPhaseRecovering, Proposal: state.Proposal}
	}
}

func BenchmarkT204SupervisedFailoverApprovalRecoveryLifecycle(b *testing.B) {
	token := []byte("0123456789abcdef")
	observation := validAutomaticFailoverObservation()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		coordinator, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{
			Enabled:               true,
			AllowOperatorOverride: true,
			OperatorOverrideToken: token,
		})
		if err != nil {
			b.Fatal(err)
		}
		proposal, err := coordinator.Propose(observation)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := coordinator.Approve(proposal, "ops", token); err != nil {
			b.Fatal(err)
		}
		if _, err := coordinator.StartRecovery(proposal); err != nil {
			b.Fatal(err)
		}
		state, err := coordinator.CompleteRecovery(proposal, SupervisedFailoverRecovery{
			CandidateID:        proposal.Decision.CandidateID,
			FencingToken:       proposal.Decision.FencingToken,
			TopologyGeneration: proposal.Decision.TopologyGeneration,
			AppliedSequence:    proposal.Decision.CandidateSequence,
		})
		if err != nil {
			b.Fatal(err)
		}
		t204SupervisedFailoverStateSink = state
	}
}
