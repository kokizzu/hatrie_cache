package hatReplication

import (
	"errors"
	"testing"
)

func TestT204SupervisedFailoverRequiresApprovalAndTracksRecovery(t *testing.T) {
	supervisor, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewSupervisedFailoverCoordinator() error = %v", err)
	}
	proposal, err := supervisor.Propose(validT204AutomaticFailoverDecision())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if _, err := supervisor.Commit(proposal); !errors.Is(err, ErrSupervisedFailoverApprovalRequired) {
		t.Fatalf("Commit() error = %v, want approval required", err)
	}

	approved, err := supervisor.Approve(proposal.Generation, "operator-a")
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if approved.Phase != SupervisedFailoverPhaseApproved || approved.Override || approved.OperatorID != "operator-a" {
		t.Fatalf("Approve() state = %+v", approved)
	}
	committed, err := supervisor.Commit(proposal)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if committed.Phase != SupervisedFailoverPhaseCommitted || committed.Generation != 2 {
		t.Fatalf("Commit() state = %+v", committed)
	}

	recovering, err := supervisor.BeginRecovery(committed.Generation, "operator-a")
	if err != nil {
		t.Fatalf("BeginRecovery() error = %v", err)
	}
	if recovering.Phase != SupervisedFailoverPhaseRecovering || recovering.Recovery.Phase != SupervisedFailoverRecoveryInProgress {
		t.Fatalf("BeginRecovery() state = %+v", recovering)
	}
	recovered, err := supervisor.CompleteRecovery(recovering.Generation, SupervisedFailoverRecoveryReport{
		NodeID:          "node-b",
		Healthy:         true,
		AppliedSequence: 100,
		FencingToken:    2,
	})
	if err != nil {
		t.Fatalf("CompleteRecovery() error = %v", err)
	}
	if recovered.Phase != SupervisedFailoverPhaseRecovered || recovered.Recovery.Phase != SupervisedFailoverRecoverySucceeded || recovered.Generation != 3 {
		t.Fatalf("CompleteRecovery() state = %+v", recovered)
	}
	reset, err := supervisor.Reset(recovered.Generation, "operator-a")
	if err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if reset.Phase != SupervisedFailoverPhaseIdle || reset.Generation != 4 || reset.HasProposal {
		t.Fatalf("Reset() state = %+v", reset)
	}
}

func TestT204SupervisedFailoverOverrideIsExplicitAndFenced(t *testing.T) {
	supervisor, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewSupervisedFailoverCoordinator() error = %v", err)
	}
	proposal, err := supervisor.Propose(validT204AutomaticFailoverDecision())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	state, err := supervisor.Override(proposal.Generation, "on-call")
	if err != nil {
		t.Fatalf("Override() error = %v", err)
	}
	if state.Phase != SupervisedFailoverPhaseApproved || !state.Override || state.OperatorID != "on-call" {
		t.Fatalf("Override() state = %+v", state)
	}
	if _, err := supervisor.Commit(SupervisedFailoverProposal{Generation: proposal.Generation, Decision: AutomaticFailoverDecision{CandidateID: "wrong"}}); !errors.Is(err, ErrSupervisedFailoverFencing) {
		t.Fatalf("Commit() error = %v, want fencing error", err)
	}
	if _, err := supervisor.Commit(proposal); err != nil {
		t.Fatalf("Commit() exact proposal error = %v", err)
	}
}

func TestT204SupervisedFailoverRecoveryFailureRequiresExplicitReset(t *testing.T) {
	supervisor, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewSupervisedFailoverCoordinator() error = %v", err)
	}
	proposal, err := supervisor.Propose(validT204AutomaticFailoverDecision())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if _, err := supervisor.Approve(proposal.Generation, "operator-a"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	committed, err := supervisor.Commit(proposal)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if _, err := supervisor.BeginRecovery(committed.Generation, "operator-a"); err != nil {
		t.Fatalf("BeginRecovery() error = %v", err)
	}
	if _, err := supervisor.CompleteRecovery(committed.Generation, SupervisedFailoverRecoveryReport{
		NodeID:          "node-c",
		Healthy:         true,
		AppliedSequence: 100,
		FencingToken:    2,
	}); !errors.Is(err, ErrSupervisedFailoverFencing) {
		t.Fatalf("CompleteRecovery() error = %v, want fencing error", err)
	}
	if _, err := supervisor.CompleteRecovery(committed.Generation, SupervisedFailoverRecoveryReport{
		NodeID:          "node-b",
		Healthy:         true,
		AppliedSequence: 99,
		FencingToken:    2,
	}); !errors.Is(err, ErrSupervisedFailoverRecoveryNotReady) {
		t.Fatalf("CompleteRecovery() error = %v, want recovery-not-ready", err)
	}
	failed, err := supervisor.FailRecovery(committed.Generation, "operator-a", "replica did not catch up")
	if err != nil {
		t.Fatalf("FailRecovery() error = %v", err)
	}
	if failed.Phase != SupervisedFailoverPhaseRecoveryFailed || failed.Recovery.Phase != SupervisedFailoverRecoveryFailed || failed.Generation != 3 {
		t.Fatalf("FailRecovery() state = %+v", failed)
	}
	if _, err := supervisor.Propose(validT204AutomaticFailoverDecision()); !errors.Is(err, ErrSupervisedFailoverPhase) {
		t.Fatalf("Propose() after failure error = %v, want phase error", err)
	}
	if _, err := supervisor.Reset(failed.Generation, "operator-a"); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
}

func TestT204SupervisedFailoverDefaultsDisabledAndRejectsStaleOperators(t *testing.T) {
	disabled, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{})
	if err != nil {
		t.Fatalf("NewSupervisedFailoverCoordinator() error = %v", err)
	}
	if disabled.Enabled() {
		t.Fatal("Enabled() = true, want false")
	}
	if _, err := disabled.Propose(validT204AutomaticFailoverDecision()); !errors.Is(err, ErrSupervisedFailoverDisabled) {
		t.Fatalf("Propose() error = %v, want disabled", err)
	}

	supervisor, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewSupervisedFailoverCoordinator() error = %v", err)
	}
	proposal, err := supervisor.Propose(validT204AutomaticFailoverDecision())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if _, err := supervisor.Approve(proposal.Generation+1, "operator-a"); !errors.Is(err, ErrSupervisedFailoverGeneration) {
		t.Fatalf("Approve() error = %v, want generation error", err)
	}
	if _, err := supervisor.Approve(proposal.Generation, " "); !errors.Is(err, ErrSupervisedFailoverOperator) {
		t.Fatalf("Approve() error = %v, want operator error", err)
	}
	if _, err := supervisor.Reject(proposal.Generation, "operator-a", "manual hold"); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	if _, err := supervisor.Commit(proposal); !errors.Is(err, ErrSupervisedFailoverPhase) {
		t.Fatalf("Commit() after reject error = %v, want phase error", err)
	}
}

func validT204AutomaticFailoverDecision() AutomaticFailoverDecision {
	return AutomaticFailoverDecision{
		SourceID:           "node-a",
		CandidateID:        "node-b",
		SourceSequence:     100,
		CandidateSequence:  99,
		FencingToken:       2,
		TopologyGeneration: 4,
		QuorumSize:         2,
		HealthyVoterCount:  2,
	}
}
