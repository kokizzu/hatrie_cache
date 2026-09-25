package hatTopology

import (
	"errors"
	"reflect"
	"testing"
)

func TestTT005RaftConfigurationJointConsensusRequiresBothQuorums(t *testing.T) {
	state, err := NewRaftConfigurationState([]string{"node-a", "node-b", "node-c"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.ProposeMembershipChange(4, RaftConfigurationChange{Kind: RaftConfigurationAddVoter, NodeID: "node-d"})
	if err != nil {
		t.Fatalf("ProposeMembershipChange() error = %v", err)
	}
	if !entry.Configuration.IsJoint() || entry.Configuration.Index != 1 || entry.Configuration.Term != 4 {
		t.Fatalf("joint entry = %#v", entry.Configuration)
	}
	if err := state.Apply(entry); err != nil {
		t.Fatalf("Apply(joint) error = %v", err)
	}
	joint := state.Snapshot()
	if got, want := joint.Voters, []string{"node-a", "node-b", "node-c", "node-d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("joint voters = %#v, want %#v", got, want)
	}
	decision, err := joint.EvaluateQuorum([]string{"node-a", "node-b", "node-d"})
	if err != nil {
		t.Fatalf("EvaluateQuorum() error = %v", err)
	}
	if !decision.Satisfied || decision.OldAcknowledged != 2 || decision.NewAcknowledged != 3 {
		t.Fatalf("joint quorum decision = %#v, want both majorities", decision)
	}
	decision, err = joint.EvaluateQuorum([]string{"node-a", "node-b"})
	if err != nil {
		t.Fatalf("EvaluateQuorum(partial) error = %v", err)
	}
	if decision.Satisfied {
		t.Fatalf("partial joint quorum = %#v, want unsatisfied", decision)
	}

	finalize, err := state.ProposeMembershipFinalize(4)
	if err != nil {
		t.Fatalf("ProposeMembershipFinalize() error = %v", err)
	}
	if finalize.Configuration.IsJoint() || finalize.Configuration.Index != 2 {
		t.Fatalf("finalize entry = %#v", finalize.Configuration)
	}
	if err := state.Apply(finalize); err != nil {
		t.Fatalf("Apply(finalize) error = %v", err)
	}
	if got := state.Snapshot(); got.IsJoint() || got.Index != 2 || !reflect.DeepEqual(got.Voters, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("final configuration = %#v", got)
	}
}

func TestTT005RaftConfigurationAppliesLearnerChangesAndRejectsStaleEntries(t *testing.T) {
	state, err := NewRaftConfigurationState([]string{"node-a", "node-b", "node-c"})
	if err != nil {
		t.Fatal(err)
	}
	addLearner, err := state.ProposeMembershipChange(1, RaftConfigurationChange{Kind: RaftConfigurationAddLearner, NodeID: "node-d"})
	if err != nil {
		t.Fatalf("add learner proposal = %v", err)
	}
	if err := state.Apply(addLearner); err != nil {
		t.Fatalf("Apply(add learner) = %v", err)
	}
	if got := state.Snapshot().Learners; !reflect.DeepEqual(got, []string{"node-d"}) {
		t.Fatalf("learners = %#v, want node-d", got)
	}
	promote, err := state.ProposeMembershipChange(2, RaftConfigurationChange{Kind: RaftConfigurationPromoteLearner, NodeID: "node-d"})
	if err != nil {
		t.Fatalf("promote learner proposal = %v", err)
	}
	if err := state.Apply(promote); err != nil {
		t.Fatalf("Apply(promote) = %v", err)
	}
	if got := state.Snapshot(); !got.IsJoint() || !reflect.DeepEqual(got.Voters, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("promoted joint configuration = %#v", got)
	}
	if _, err := state.ProposeMembershipChange(2, RaftConfigurationChange{Kind: RaftConfigurationAddVoter, NodeID: "node-e"}); !errors.Is(err, ErrRaftConfigurationJointPending) {
		t.Fatalf("proposal during joint state = %v, want joint-pending error", err)
	}
	if err := state.Apply(addLearner); !errors.Is(err, ErrRaftConfigurationIndex) {
		t.Fatalf("stale entry error = %v, want index error", err)
	}
}

func TestTT005RaftConfigurationValidatesChangesAndClonesSnapshots(t *testing.T) {
	state, err := NewRaftConfigurationState([]string{"node-a", "node-b", "node-c"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRaftConfigurationState([]string{"node-a", "node-a"}); !errors.Is(err, ErrRaftConfigurationInvalid) {
		t.Fatalf("duplicate initial voter error = %v", err)
	}
	if _, err := state.ProposeMembershipChange(1, RaftConfigurationChange{Kind: RaftConfigurationRemoveVoter, NodeID: "node-a"}); err != nil {
		t.Fatalf("remove voter proposal = %v", err)
	}
	if _, err := state.ProposeMembershipChange(1, RaftConfigurationChange{Kind: RaftConfigurationAddVoter, NodeID: "node-a"}); !errors.Is(err, ErrRaftConfigurationProposal) {
		t.Fatalf("second proposal error = %v, want proposal error", err)
	}
	snapshot := state.Snapshot()
	snapshot.Voters[0] = "mutated"
	if state.Snapshot().Voters[0] == "mutated" {
		t.Fatal("Snapshot() exposed mutable voter storage")
	}
	if _, err := snapshot.EvaluateQuorum([]string{"unknown"}); !errors.Is(err, ErrRaftConfigurationMember) {
		t.Fatalf("unknown quorum member error = %v, want member error", err)
	}
}
