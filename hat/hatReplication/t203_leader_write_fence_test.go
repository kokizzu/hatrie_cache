package hatReplication

import (
	"errors"
	"testing"
)

func TestT203LeaderWriteFenceRejectsStaleWriterAfterAdvance(t *testing.T) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v", err)
	}
	var writes int
	oldWrite := ReplicaSetLeaderWrite{NodeID: "node-a", Term: 1, FencingToken: 1}
	if err := fence.Execute(oldWrite, func() error {
		writes++
		return nil
	}); err != nil {
		t.Fatalf("initial Execute() error = %v", err)
	}

	state, err := fence.Advance(ReplicaSetLeaderWriteFenceTransition{
		LeaderID:     "node-b",
		Term:         2,
		FencingToken: 2,
	})
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if state.LeaderID != "node-b" || state.Term != 2 || state.FencingToken != 2 || state.Generation != 2 {
		t.Fatalf("Advance() state = %+v", state)
	}

	for name, write := range map[string]ReplicaSetLeaderWrite{
		"old leader": oldWrite,
		"new leader with old term": {
			NodeID: "node-b", Term: 1, FencingToken: 2,
		},
		"new leader with old token": {
			NodeID: "node-b", Term: 2, FencingToken: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := fence.Execute(write, func() error {
				writes++
				return nil
			}); !errors.Is(err, ErrReplicaSetLeaderWriteFenceStale) {
				t.Fatalf("Execute() error = %v, want stale fence", err)
			}
		})
	}

	if err := fence.Execute(ReplicaSetLeaderWrite{NodeID: "node-b", Term: 2, FencingToken: 2}, func() error {
		writes++
		return nil
	}); err != nil {
		t.Fatalf("new leader Execute() error = %v", err)
	}
	if writes != 2 {
		t.Fatalf("callback writes = %d, want 2", writes)
	}
}

func TestT203LeaderWriteFenceIsDisabledByDefault(t *testing.T) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v", err)
	}
	if fence.Enabled() {
		t.Fatal("Enabled() = true, want false")
	}
	if err := fence.Execute(ReplicaSetLeaderWrite{}, func() error { return nil }); !errors.Is(err, ErrReplicaSetLeaderWriteFenceDisabled) {
		t.Fatalf("Execute() error = %v, want disabled", err)
	}
	if _, err := fence.Advance(ReplicaSetLeaderWriteFenceTransition{LeaderID: "node-a", Term: 1, FencingToken: 1}); !errors.Is(err, ErrReplicaSetLeaderWriteFenceDisabled) {
		t.Fatalf("Advance() error = %v, want disabled", err)
	}
}

func TestT203LeaderWriteFenceRequiresMonotoneTransitions(t *testing.T) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         4,
		InitialFencingToken: 8,
	})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v", err)
	}
	for name, transition := range map[string]ReplicaSetLeaderWriteFenceTransition{
		"lower term":  {LeaderID: "node-b", Term: 3, FencingToken: 9},
		"same term":   {LeaderID: "node-b", Term: 4, FencingToken: 9},
		"lower token": {LeaderID: "node-b", Term: 5, FencingToken: 7},
		"same token":  {LeaderID: "node-b", Term: 5, FencingToken: 8},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fence.Advance(transition); !errors.Is(err, ErrReplicaSetLeaderWriteFenceStale) {
				t.Fatalf("Advance() error = %v, want stale transition", err)
			}
		})
	}
	if _, err := fence.Advance(ReplicaSetLeaderWriteFenceTransition{LeaderID: "node-b", Term: 5, FencingToken: 9}); err != nil {
		t.Fatalf("valid Advance() error = %v", err)
	}
}

func TestT203LeaderWriteFencePropagatesWriteError(t *testing.T) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v", err)
	}
	want := errors.New("write failed")
	if err := fence.Execute(ReplicaSetLeaderWrite{NodeID: "node-a", Term: 1, FencingToken: 1}, func() error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("Execute() error = %v, want %v", err, want)
	}
	if state := fence.Snapshot(); state.LeaderID != "node-a" || state.Term != 1 || state.FencingToken != 1 {
		t.Fatalf("Snapshot() after failed write = %+v", state)
	}
}

func TestT203LeaderWriteFenceValidatesInitialStateAndNoLeader(t *testing.T) {
	for name, options := range map[string]ReplicaSetLeaderWriteFenceOptions{
		"leader without term": {
			Enabled:             true,
			InitialLeader:       "node-a",
			InitialFencingToken: 1,
		},
		"term without leader": {
			Enabled:     true,
			InitialTerm: 1,
		},
		"token without leader": {
			Enabled:             true,
			InitialFencingToken: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewReplicaSetLeaderWriteFence(options); !errors.Is(err, ErrReplicaSetLeaderWriteFenceInvalid) {
				t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v, want invalid", err)
			}
		})
	}
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewReplicaSetLeaderWriteFence() error = %v", err)
	}
	if err := fence.Execute(ReplicaSetLeaderWrite{NodeID: "node-a", Term: 1, FencingToken: 1}, func() error { return nil }); !errors.Is(err, ErrReplicaSetLeaderWriteFenceNoLeader) {
		t.Fatalf("Execute() error = %v, want no leader", err)
	}
}
