//go:build t210

package hatReplication_test

import (
	"errors"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestT210ConflictHookReceivesSourceAndSequenceContext(t *testing.T) {
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 41}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	var observed hatReplication.ConflictHookContext
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{
		Mode: hatReplication.ConflictPolicyLastWriteWins,
		Hook: func(context hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			observed = context
			return hatReplication.ConflictHookUseLeft, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	winner, err := registry.Resolve("orders", left, right)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if winner != left {
		t.Fatalf("Resolve() = %#v, want hook-selected left version", winner)
	}
	if observed.Space != "orders" || observed.Left != left || observed.Right != right {
		t.Fatalf("hook context = %#v, want space and both source/sequence versions", observed)
	}
}

func TestT210ConflictHookCanDelegateRejectAndPropagateErrors(t *testing.T) {
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-b", Sequence: 2}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{
		Mode: hatReplication.ConflictPolicyLastWriteWins,
		Hook: func(hatrie_cache hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			return hatReplication.ConflictHookReject, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("orders", left, right); !errors.Is(err, hatReplication.ErrConflictRejected) {
		t.Fatalf("hook reject error = %v, want ErrConflictRejected", err)
	}

	wantErr := errors.New("hook failed")
	registry, err = hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{
		Hook: func(hatrie_cache hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			return hatReplication.ConflictHookUsePolicy, wantErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("orders", left, right); !errors.Is(err, wantErr) {
		t.Fatalf("hook error = %v, want %v", err, wantErr)
	}
}

func TestT210ConflictHookDoesNotRunForEqualVersions(t *testing.T) {
	version := hatReplication.ConflictVersion{Timestamp: 5, NodeID: "node-a", Sequence: 7}
	called := false
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{
		Hook: func(hatrie_cache hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			called = true
			return hatReplication.ConflictHookReject, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	winner, err := registry.Resolve("orders", version, version)
	if err != nil || winner != version {
		t.Fatalf("equal Resolve() = %#v/%v, want equal version/nil", winner, err)
	}
	if called {
		t.Fatal("hook was called for an equal version")
	}
}

func TestT210ConflictHookRejectsInvalidDecision(t *testing.T) {
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{
		Hook: func(hatrie_cache hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			return hatReplication.ConflictHookDecision(99), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("orders", left, right); !errors.Is(err, hatReplication.ErrConflictHookInvalid) {
		t.Fatalf("invalid hook decision error = %v, want ErrConflictHookInvalid", err)
	}
}

func TestT210ZeroValueRegistryKeepsDefaultConflictPolicy(t *testing.T) {
	var registry hatReplication.ConflictPolicyRegistry
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	winner, err := registry.Resolve("orders", left, right)
	if err != nil || winner != right {
		t.Fatalf("zero-value Resolve() = %#v/%v, want right version/nil", winner, err)
	}
}
