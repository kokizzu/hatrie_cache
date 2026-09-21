package hatReplication

import "testing"

func BenchmarkTT005ProposeCommitJointConfiguration(b *testing.B) {
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-b", "node-c", "node-d"},
		InitialLearners: []string{"node-e"},
	})
	if err != nil {
		b.Fatal(err)
	}
	promote := RaftConfigurationChange{AddVoters: []string{"node-e"}, RemoveLearners: []string{"node-e"}}
	demote := RaftConfigurationChange{AddLearners: []string{"node-e"}, RemoveVoters: []string{"node-e"}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		change := promote
		if index%2 == 1 {
			change = demote
		}
		entry, err := state.Propose(change, uint64(index+1))
		if err != nil {
			b.Fatal(err)
		}
		acknowledgements := tt005Acks(entry, "node-a", "node-b", "node-c", "node-e")
		if index%2 == 1 {
			acknowledgements = tt005Acks(entry, "node-a", "node-b", "node-c")
		}
		if _, err := state.Commit(entry, acknowledgements); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTT005SnapshotDetachedConfiguration(b *testing.B) {
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-b", "node-c", "node-d"},
		InitialLearners: []string{"node-e", "node-f"},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = state.Snapshot()
	}
}
