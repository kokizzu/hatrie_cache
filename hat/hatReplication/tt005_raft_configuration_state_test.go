package hatReplication

import (
	"errors"
	"reflect"
	"testing"
)

func TestTT005JointConfigurationRequiresBothQuorums(t *testing.T) {
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-b", "node-c"},
		InitialLearners: []string{"node-d"},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Propose(RaftConfigurationChange{
		AddVoters:      []string{"node-d"},
		RemoveLearners: []string{"node-d"},
	}, 2)
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if entry.Index != 1 || entry.Term != 2 || !reflect.DeepEqual(entry.Previous.Voters, []string{"node-a", "node-b", "node-c"}) || !reflect.DeepEqual(entry.Next.Voters, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("Propose() entry = %+v", entry)
	}
	if _, err := state.Commit(entry, tt005Acks(entry, "node-a", "node-b")); !errors.Is(err, ErrRaftConfigurationStateQuorum) {
		t.Fatalf("partial joint quorum error = %v, want quorum error", err)
	}
	if snapshot := state.Snapshot(); snapshot.Pending == nil || !reflect.DeepEqual(snapshot.Current.Voters, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("failed commit changed state = %+v", snapshot)
	}
	committed, err := state.Commit(entry, tt005Acks(entry, "node-a", "node-b", "node-d"))
	if err != nil {
		t.Fatalf("joint Commit() error = %v", err)
	}
	if committed.Pending != nil || !reflect.DeepEqual(committed.Current.Voters, []string{"node-a", "node-b", "node-c", "node-d"}) || len(committed.Current.Learners) != 0 {
		t.Fatalf("joint Commit() snapshot = %+v", committed)
	}
}

func TestTT005ConfigurationChangePromotesAndDemotesSafely(t *testing.T) {
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-b", "node-c", "node-d"},
		InitialLearners: []string{"node-e"},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Propose(RaftConfigurationChange{
		AddVoters:      []string{"node-e"},
		RemoveLearners: []string{"node-e"},
	}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Commit(entry, tt005Acks(entry, "node-a", "node-b", "node-c", "node-e")); err != nil {
		t.Fatalf("promote learner error = %v", err)
	}
	demote, err := state.Propose(RaftConfigurationChange{
		AddLearners:  []string{"node-e"},
		RemoveVoters: []string{"node-e"},
	}, 5)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := state.Commit(demote, tt005Acks(demote, "node-a", "node-b", "node-c"))
	if err != nil {
		t.Fatalf("demote learner error = %v", err)
	}
	if !reflect.DeepEqual(committed.Current.Voters, []string{"node-a", "node-b", "node-c", "node-d"}) || !reflect.DeepEqual(committed.Current.Learners, []string{"node-e"}) {
		t.Fatalf("demote snapshot = %+v", committed)
	}
}

func TestTT005ConfigurationStateRejectsInvalidAndStaleOperations(t *testing.T) {
	if _, err := NewRaftConfigurationState(RaftConfigurationStateOptions{}); !errors.Is(err, ErrRaftConfigurationStateInvalid) {
		t.Fatalf("empty configuration error = %v", err)
	}
	if _, err := NewRaftConfigurationState(RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-a"},
		InitialLearners: []string{"node-a"},
	}); !errors.Is(err, ErrRaftConfigurationStateInvalid) {
		t.Fatalf("duplicate/overlapping configuration error = %v", err)
	}
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{InitialVoters: []string{"node-a", "node-b", "node-c"}, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Propose(RaftConfigurationChange{}, 1); !errors.Is(err, ErrRaftConfigurationStateNoChange) {
		t.Fatalf("no-op change error = %v", err)
	}
	if _, err := state.Propose(RaftConfigurationChange{AddVoters: []string{"node-d"}}, 0); !errors.Is(err, ErrRaftConfigurationStateInvalid) {
		t.Fatalf("zero term error = %v", err)
	}
	entry, err := state.Propose(RaftConfigurationChange{AddVoters: []string{"node-d"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Propose(RaftConfigurationChange{AddLearners: []string{"node-e"}}, 2); !errors.Is(err, ErrRaftConfigurationStatePending) {
		t.Fatalf("pending proposal error = %v", err)
	}
	badAck := tt005Acks(entry, "node-a")
	badAck[0].Index++
	if _, err := state.Commit(entry, badAck); !errors.Is(err, ErrRaftConfigurationStateAcknowledgement) {
		t.Fatalf("stale acknowledgement error = %v", err)
	}
	if _, err := state.Commit(entry, tt005Acks(entry, "node-a", "node-b", "node-c")); err != nil {
		t.Fatalf("joint quorum with three shared voters error = %v", err)
	}
}

func TestTT005ConfigurationStateSnapshotsAreDetached(t *testing.T) {
	state, err := NewRaftConfigurationState(RaftConfigurationStateOptions{InitialVoters: []string{"node-b", "node-a", "node-c"}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	snapshot.Current.Voters[0] = "mutated"
	if got := state.Snapshot().Current.Voters[0]; got != "node-a" {
		t.Fatalf("snapshot mutation changed state to %q", got)
	}
}

func tt005Acks(entry RaftConfigurationEntry, nodes ...string) []RaftConfigurationAcknowledgement {
	acks := make([]RaftConfigurationAcknowledgement, len(nodes))
	for index, node := range nodes {
		acks[index] = RaftConfigurationAcknowledgement{
			Node:     node,
			Index:    entry.Index,
			Term:     entry.Term,
			Accepted: true,
		}
	}
	return acks
}
