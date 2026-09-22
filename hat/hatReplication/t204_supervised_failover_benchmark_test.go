package hatReplication

import "testing"

var t204DecisionSink AutomaticFailoverDecision

func BenchmarkT204ExistingAutomaticFailoverEvaluation(b *testing.B) {
	options := AutomaticFailoverOptions{Enabled: true, MaxCandidates: DefaultAutomaticFailoverMaxCandidates}
	observation := AutomaticFailoverObservation{
		SourceID:           "node-a",
		SourceSequence:     100,
		FencingToken:       1,
		TopologyGeneration: 3,
		VoterCount:         3,
		HealthyVoterCount:  2,
		Candidates: []AutomaticFailoverCandidate{
			{NodeID: "node-b", Healthy: true, AppliedSequence: 100},
			{NodeID: "node-c", Healthy: true, AppliedSequence: 99},
		},
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision, err := evaluateAutomaticFailover(options, observation)
		if err != nil {
			b.Fatal(err)
		}
		t204DecisionSink = decision
	}
}

var t204StateSink SupervisedFailoverState

func BenchmarkT204SupervisedApprovalLifecycle(b *testing.B) {
	benchmarkT204SupervisedLifecycle(b, false)
}

func BenchmarkT204SupervisedOverrideLifecycle(b *testing.B) {
	benchmarkT204SupervisedLifecycle(b, true)
}

func benchmarkT204SupervisedLifecycle(b *testing.B, override bool) {
	supervisor, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	decision := AutomaticFailoverDecision{
		SourceID:           "node-a",
		CandidateID:        "node-b",
		SourceSequence:     100,
		CandidateSequence:  99,
		FencingToken:       2,
		TopologyGeneration: 4,
		QuorumSize:         2,
		HealthyVoterCount:  2,
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proposal, err := supervisor.Propose(decision)
		if err != nil {
			b.Fatal(err)
		}
		if override {
			if _, err := supervisor.Override(proposal.Generation, "operator"); err != nil {
				b.Fatal(err)
			}
		} else if _, err := supervisor.Approve(proposal.Generation, "operator"); err != nil {
			b.Fatal(err)
		}
		committed, err := supervisor.Commit(proposal)
		if err != nil {
			b.Fatal(err)
		}
		recovering, err := supervisor.BeginRecovery(committed.Generation, "operator")
		if err != nil {
			b.Fatal(err)
		}
		recovered, err := supervisor.CompleteRecovery(recovering.Generation, SupervisedFailoverRecoveryReport{
			NodeID:          "node-b",
			Healthy:         true,
			AppliedSequence: 100,
			FencingToken:    2,
		})
		if err != nil {
			b.Fatal(err)
		}
		state, err := supervisor.Reset(recovered.Generation, "operator")
		if err != nil {
			b.Fatal(err)
		}
		t204StateSink = state
	}
}
