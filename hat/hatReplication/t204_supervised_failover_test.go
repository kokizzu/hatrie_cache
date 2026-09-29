package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestSupervisedFailoverIsDisabledByDefault(t *testing.T) {
	coordinator, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Propose(validAutomaticFailoverObservation()); !errors.Is(err, ErrSupervisedFailoverDisabled) {
		t.Fatalf("Propose() error = %v, want ErrSupervisedFailoverDisabled", err)
	}
}

func TestSupervisedFailoverRequiresApprovalAndRecordsRecovery(t *testing.T) {
	token := []byte("0123456789abcdef")
	coordinator, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{
		Enabled: true,
		Automatic: AutomaticFailoverOptions{
			MaxLag: 1,
		},
		AllowOperatorOverride: true,
		OperatorOverrideToken: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := coordinator.Propose(validAutomaticFailoverObservation())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if state := coordinator.Snapshot(); state.Phase != SupervisedFailoverPhaseAwaitingApproval || !state.HasProposal {
		t.Fatalf("awaiting approval state = %+v", state)
	}

	if _, err := coordinator.Approve(proposal, "ops", []byte("wrong-token")); !errors.Is(err, ErrSupervisedFailoverOverrideInvalid) {
		t.Fatalf("invalid token error = %v, want ErrSupervisedFailoverOverrideInvalid", err)
	}
	if _, err := coordinator.Approve(proposal, "", token); !errors.Is(err, ErrSupervisedFailoverOperatorInvalid) {
		t.Fatalf("invalid operator error = %v, want ErrSupervisedFailoverOperatorInvalid", err)
	}
	stale := proposal
	stale.Generation++
	if _, err := coordinator.Approve(stale, "ops", token); !errors.Is(err, ErrSupervisedFailoverGeneration) {
		t.Fatalf("stale approval error = %v, want ErrSupervisedFailoverGeneration", err)
	}

	state, err := coordinator.Approve(proposal, "ops@example", token)
	if err != nil || state.Phase != SupervisedFailoverPhaseApproved || state.OperatorID != "ops@example" {
		t.Fatalf("Approve() = %+v, %v", state, err)
	}
	state, err = coordinator.StartRecovery(proposal)
	if err != nil || state.Phase != SupervisedFailoverPhaseRecovering {
		t.Fatalf("StartRecovery() = %+v, %v", state, err)
	}

	invalidRecovery := SupervisedFailoverRecovery{
		CandidateID:        "node-c",
		FencingToken:       proposal.Decision.FencingToken,
		TopologyGeneration: proposal.Decision.TopologyGeneration,
		AppliedSequence:    proposal.Decision.CandidateSequence,
	}
	if _, err := coordinator.CompleteRecovery(proposal, invalidRecovery); !errors.Is(err, ErrSupervisedFailoverRecoveryInvalid) {
		t.Fatalf("invalid recovery error = %v, want ErrSupervisedFailoverRecoveryInvalid", err)
	}
	recovery := SupervisedFailoverRecovery{
		CandidateID:        proposal.Decision.CandidateID,
		FencingToken:       proposal.Decision.FencingToken,
		TopologyGeneration: proposal.Decision.TopologyGeneration,
		AppliedSequence:    proposal.Decision.CandidateSequence + 1,
	}
	state, err = coordinator.CompleteRecovery(proposal, recovery)
	if err != nil || state.Phase != SupervisedFailoverPhaseRecovered || !state.HasRecovery || state.Recovery != recovery {
		t.Fatalf("CompleteRecovery() = %+v, %v", state, err)
	}
	if _, err := coordinator.StartRecovery(proposal); !errors.Is(err, ErrSupervisedFailoverPhase) {
		t.Fatalf("second StartRecovery() error = %v, want ErrSupervisedFailoverPhase", err)
	}
}

func TestSupervisedFailoverAbortFencesRecovery(t *testing.T) {
	token := []byte("0123456789abcdef")
	coordinator, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{
		Enabled:               true,
		AllowOperatorOverride: true,
		OperatorOverrideToken: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := coordinator.Propose(validAutomaticFailoverObservation())
	if err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.Abort(proposal, "on-call", token, "source recovered")
	if err != nil || state.Phase != SupervisedFailoverPhaseAborted || state.AbortReason != "source recovered" {
		t.Fatalf("Abort() = %+v, %v", state, err)
	}
	if _, err := coordinator.Approve(proposal, "on-call", token); !errors.Is(err, ErrSupervisedFailoverPhase) {
		t.Fatalf("post-abort Approve() error = %v, want ErrSupervisedFailoverPhase", err)
	}
}

func TestSupervisedFailoverSnapshotsAreSafeDuringProposal(t *testing.T) {
	token := []byte("0123456789abcdef")
	coordinator, err := NewSupervisedFailoverCoordinator(SupervisedFailoverOptions{
		Enabled:               true,
		AllowOperatorOverride: true,
		OperatorOverrideToken: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = coordinator.Snapshot()
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		_, _ = coordinator.Propose(validAutomaticFailoverObservation())
	}()
	group.Wait()
	if state := coordinator.Snapshot(); state.Phase != SupervisedFailoverPhaseAwaitingApproval {
		t.Fatalf("concurrent final state = %+v", state)
	}
}
