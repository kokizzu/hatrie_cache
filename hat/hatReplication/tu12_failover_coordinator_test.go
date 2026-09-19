package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestAutomaticFailoverIsDisabledByDefault(t *testing.T) {
	coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Propose(validAutomaticFailoverObservation()); !errors.Is(err, ErrAutomaticFailoverDisabled) {
		t.Fatalf("Propose() error = %v, want ErrAutomaticFailoverDisabled", err)
	}
	state := coordinator.Snapshot()
	if state.Phase != AutomaticFailoverPhaseIdle || state.Generation != 0 {
		t.Fatalf("disabled state = %+v", state)
	}
}

func TestAutomaticFailoverProposesDeterministicallyAndCommits(t *testing.T) {
	coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := coordinator.Propose(validAutomaticFailoverObservation())
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if proposal.Generation != 1 || proposal.Decision.CandidateID != "node-b" || proposal.Decision.FencingToken != 8 || proposal.Decision.TopologyGeneration != 5 || proposal.Decision.QuorumSize != 2 {
		t.Fatalf("proposal = %+v", proposal)
	}
	state := coordinator.Snapshot()
	if state.Phase != AutomaticFailoverPhaseProposed || state.Generation != 1 || !state.HasProposal {
		t.Fatalf("proposed state = %+v", state)
	}
	stale := proposal
	stale.Generation++
	if _, err := coordinator.Commit(stale); !errors.Is(err, ErrAutomaticFailoverGeneration) {
		t.Fatalf("stale Commit() error = %v", err)
	}
	state, err = coordinator.Commit(proposal)
	if err != nil || state.Phase != AutomaticFailoverPhaseCommitted || state.Generation != 2 {
		t.Fatalf("Commit() = %+v, %v", state, err)
	}
	if _, err := coordinator.Commit(proposal); !errors.Is(err, ErrAutomaticFailoverPhase) {
		t.Fatalf("second Commit() error = %v", err)
	}
}

func TestAutomaticFailoverRejectsUnsafeObservations(t *testing.T) {
	tests := []struct {
		name string
		edit func(*AutomaticFailoverObservation)
		want error
	}{
		{name: "source healthy", edit: func(observation *AutomaticFailoverObservation) { observation.SourceHealthy = true }, want: ErrAutomaticFailoverSourceHealthy},
		{name: "no quorum", edit: func(observation *AutomaticFailoverObservation) { observation.HealthyVoterCount = 1 }, want: ErrAutomaticFailoverNoQuorum},
		{name: "candidate lag", edit: func(observation *AutomaticFailoverObservation) {
			observation.Candidates[0].AppliedSequence = 99
			observation.Candidates[1].AppliedSequence = 98
		}, want: ErrAutomaticFailoverNoCandidate},
		{name: "zero fence", edit: func(observation *AutomaticFailoverObservation) { observation.FencingToken = 0 }, want: ErrAutomaticFailoverInvalid},
		{name: "duplicate candidate", edit: func(observation *AutomaticFailoverObservation) {
			observation.Candidates[1].NodeID = observation.Candidates[0].NodeID
		}, want: ErrAutomaticFailoverInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
			if err != nil {
				t.Fatal(err)
			}
			observation := validAutomaticFailoverObservation()
			test.edit(&observation)
			if _, err := coordinator.Propose(observation); !errors.Is(err, test.want) {
				t.Fatalf("Propose() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestAutomaticFailoverCancelFencesStaleProposal(t *testing.T) {
	coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := coordinator.Propose(validAutomaticFailoverObservation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Cancel(proposal.Generation + 1); !errors.Is(err, ErrAutomaticFailoverGeneration) {
		t.Fatalf("stale Cancel() error = %v", err)
	}
	state, err := coordinator.Cancel(proposal.Generation)
	if err != nil || state.Phase != AutomaticFailoverPhaseCancelled || state.Generation != 2 {
		t.Fatalf("Cancel() = %+v, %v", state, err)
	}
	if _, err := coordinator.Commit(proposal); !errors.Is(err, ErrAutomaticFailoverPhase) {
		t.Fatalf("post-cancel Commit() error = %v", err)
	}
}

func TestAutomaticFailoverSnapshotsAreSafeDuringProposal(t *testing.T) {
	coordinator, err := NewAutomaticFailoverCoordinator(AutomaticFailoverOptions{Enabled: true})
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
	if state := coordinator.Snapshot(); state.Phase != AutomaticFailoverPhaseProposed {
		t.Fatalf("concurrent final state = %+v", state)
	}
}

func validAutomaticFailoverObservation() AutomaticFailoverObservation {
	return AutomaticFailoverObservation{
		SourceID:           "node-a",
		SourceSequence:     100,
		FencingToken:       7,
		TopologyGeneration: 4,
		VoterCount:         3,
		HealthyVoterCount:  2,
		Candidates: []AutomaticFailoverCandidate{
			{NodeID: "node-c", Healthy: true, AppliedSequence: 100},
			{NodeID: "node-b", Healthy: true, AppliedSequence: 100},
		},
	}
}
