package hatTopology

import "testing"

func tt005BenchmarkConfiguration(joint bool) RaftConfiguration {
	state, err := NewRaftConfigurationState([]string{"node-a", "node-b", "node-c", "node-d", "node-e"})
	if err != nil {
		panic(err)
	}
	if !joint {
		return state.Snapshot()
	}
	entry, err := state.ProposeMembershipChange(2, RaftConfigurationChange{Kind: RaftConfigurationAddVoter, NodeID: "node-f"})
	if err != nil {
		panic(err)
	}
	if err := state.Apply(entry); err != nil {
		panic(err)
	}
	return state.Snapshot()
}

func BenchmarkTT005StableQuorum(b *testing.B) {
	configuration := tt005BenchmarkConfiguration(false)
	acknowledgements := []string{"node-a", "node-b", "node-c"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decision, err := configuration.EvaluateQuorum(acknowledgements)
		if err != nil || !decision.Satisfied {
			b.Fatalf("stable quorum = %#v/%v", decision, err)
		}
	}
}

func BenchmarkTT005JointQuorum(b *testing.B) {
	configuration := tt005BenchmarkConfiguration(true)
	acknowledgements := []string{"node-a", "node-b", "node-c", "node-f"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decision, err := configuration.EvaluateQuorum(acknowledgements)
		if err != nil || !decision.Satisfied {
			b.Fatalf("joint quorum = %#v/%v", decision, err)
		}
	}
}

func BenchmarkTT005ProposalAndApply(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		state, err := NewRaftConfigurationState([]string{"node-a", "node-b", "node-c"})
		if err != nil {
			b.Fatal(err)
		}
		entry, err := state.ProposeMembershipChange(1, RaftConfigurationChange{Kind: RaftConfigurationAddVoter, NodeID: "node-d"})
		if err != nil {
			b.Fatal(err)
		}
		if err := state.Apply(entry); err != nil {
			b.Fatal(err)
		}
	}
}
