package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestT202ReplicaSetLeaderElectionChoosesFreshestCandidateAndFencesCommit(t *testing.T) {
	now := time.Unix(100, 0)
	election, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{
		Enabled:         true,
		Voters:          []string{"node-a", "node-b", "node-c"},
		ElectionTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderElection() error = %v", err)
	}
	for _, heartbeat := range []ReplicaSetLeaderHeartbeat{
		{NodeID: "node-a", AppliedSequence: 10, Healthy: true},
		{NodeID: "node-b", AppliedSequence: 12, Healthy: true},
		{NodeID: "node-c", AppliedSequence: 11, Healthy: true},
	} {
		if err := election.ObserveHeartbeat(now, heartbeat); err != nil {
			t.Fatalf("ObserveHeartbeat(%+v) error = %v", heartbeat, err)
		}
	}
	proposal, err := election.TryElect(now)
	if err != nil {
		t.Fatalf("TryElect() error = %v", err)
	}
	if proposal.LeaderID != "node-b" || proposal.Term != 1 || proposal.RequiredQuorum != 2 || proposal.HealthyVoters != 3 {
		t.Fatalf("proposal = %+v, want node-b term 1 quorum 2 healthy 3", proposal)
	}
	if _, err := election.TryElect(now); !errors.Is(err, ErrReplicaSetLeaderElectionPending) {
		t.Fatalf("second TryElect() error = %v, want pending", err)
	}
	state, err := election.Commit(now, proposal)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if state.LeaderID != "node-b" || state.Term != 1 || state.Generation != 2 || state.HasPending {
		t.Fatalf("committed state = %+v, want node-b term 1 generation 2 without pending", state)
	}
}

func TestT202ReplicaSetLeaderElectionDefaultsDisabledAndRequiresQuorum(t *testing.T) {
	now := time.Unix(200, 0)
	disabled, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{Voters: []string{"node-a"}})
	if err != nil {
		t.Fatalf("disabled constructor: %v", err)
	}
	if err := disabled.ObserveHeartbeat(now, ReplicaSetLeaderHeartbeat{NodeID: "node-a", Healthy: true}); !errors.Is(err, ErrReplicaSetLeaderElectionDisabled) {
		t.Fatalf("disabled ObserveHeartbeat() error = %v, want disabled", err)
	}
	if _, err := disabled.TryElect(now); !errors.Is(err, ErrReplicaSetLeaderElectionDisabled) {
		t.Fatalf("disabled TryElect() error = %v, want disabled", err)
	}

	election, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{Enabled: true, Voters: []string{"node-a", "node-b", "node-c"}})
	if err != nil {
		t.Fatalf("enabled constructor: %v", err)
	}
	if err := election.ObserveHeartbeat(now, ReplicaSetLeaderHeartbeat{NodeID: "node-a", AppliedSequence: 9, Healthy: true}); err != nil {
		t.Fatalf("single heartbeat: %v", err)
	}
	if _, err := election.TryElect(now); !errors.Is(err, ErrReplicaSetLeaderElectionNoQuorum) {
		t.Fatalf("one-voter TryElect() error = %v, want no quorum", err)
	}
}

func TestT202ReplicaSetLeaderElectionReelectsAfterHeartbeatExpiry(t *testing.T) {
	now := time.Unix(300, 0)
	election, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{
		Enabled:         true,
		Voters:          []string{"node-a", "node-b", "node-c"},
		ElectionTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	for _, heartbeat := range []ReplicaSetLeaderHeartbeat{
		{NodeID: "node-a", AppliedSequence: 10, Healthy: true},
		{NodeID: "node-b", AppliedSequence: 12, Healthy: true},
		{NodeID: "node-c", AppliedSequence: 11, Healthy: true},
	} {
		if err := election.ObserveHeartbeat(now, heartbeat); err != nil {
			t.Fatalf("initial heartbeat: %v", err)
		}
	}
	first, err := election.TryElect(now)
	if err != nil {
		t.Fatalf("initial TryElect(): %v", err)
	}
	if _, err := election.Commit(now, first); err != nil {
		t.Fatalf("initial Commit(): %v", err)
	}

	later := now.Add(2 * time.Second)
	for _, heartbeat := range []ReplicaSetLeaderHeartbeat{
		{NodeID: "node-a", AppliedSequence: 20, Healthy: true},
		{NodeID: "node-c", AppliedSequence: 19, Healthy: true},
	} {
		if err := election.ObserveHeartbeat(later, heartbeat); err != nil {
			t.Fatalf("replacement heartbeat: %v", err)
		}
	}
	second, err := election.TryElect(later)
	if err != nil {
		t.Fatalf("re-election TryElect(): %v", err)
	}
	if second.LeaderID != "node-a" || second.Term != 2 || second.FencingToken != 2 {
		t.Fatalf("re-election proposal = %+v, want node-a term/fence 2", second)
	}
	if _, err := election.Commit(later, second); err != nil {
		t.Fatalf("re-election Commit(): %v", err)
	}
}

func TestT202ReplicaSetLeaderElectionRejectsStaleCommitAndUnknownHeartbeat(t *testing.T) {
	now := time.Unix(400, 0)
	election, err := NewReplicaSetLeaderElection(ReplicaSetLeaderElectionOptions{Enabled: true, Voters: []string{"node-a"}})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if err := election.ObserveHeartbeat(now, ReplicaSetLeaderHeartbeat{NodeID: "unknown", Healthy: true}); !errors.Is(err, ErrReplicaSetLeaderElectionUnknownVoter) {
		t.Fatalf("unknown heartbeat error = %v, want unknown voter", err)
	}
	if err := election.ObserveHeartbeat(now, ReplicaSetLeaderHeartbeat{NodeID: "node-a", Healthy: true}); err != nil {
		t.Fatalf("known heartbeat: %v", err)
	}
	proposal, err := election.TryElect(now)
	if err != nil {
		t.Fatalf("TryElect(): %v", err)
	}
	proposal.Term++
	if _, err := election.Commit(now, proposal); !errors.Is(err, ErrReplicaSetLeaderElectionStale) {
		t.Fatalf("stale Commit() error = %v, want stale", err)
	}
}
