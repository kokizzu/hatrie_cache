package hatReplication

import (
	"errors"
	"testing"
)

func TestT210ConflictHookReceivesSourceAndSequenceContext(t *testing.T) {
	var event ConflictResolutionEvent
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 7}
	right := ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	winner, err := registry.ResolveWithHook("orders", left, right, func(observed ConflictResolutionEvent) {
		event = observed
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if winner != right {
		t.Fatalf("winner = %#v, want %#v", winner, right)
	}
	if event.Space != "orders" || event.Left != left || event.Right != right || event.Winner != right || event.Decision != ConflictResolutionRightWins {
		t.Fatalf("hook event = %#v, want orders, both versions, right winner", event)
	}
}

func TestT210RejectedConflictInvokesHookWithoutWinner(t *testing.T) {
	var event ConflictResolutionEvent
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyReject})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 2}
	right := ConflictVersion{Timestamp: 1, NodeID: "node-b", Sequence: 3}
	winner, err := registry.ResolveWithHook("orders", left, right, func(observed ConflictResolutionEvent) {
		event = observed
	})
	if !errors.Is(err, ErrConflictRejected) {
		t.Fatalf("Resolve() error = %v, want ErrConflictRejected", err)
	}
	if winner != (ConflictVersion{}) {
		t.Fatalf("winner = %#v, want zero winner", winner)
	}
	if event.Decision != ConflictResolutionRejected || event.Winner != (ConflictVersion{}) || event.Left != left || event.Right != right {
		t.Fatalf("rejected hook event = %#v", event)
	}
}

func TestT210EqualVersionsDoNotInvokeConflictHook(t *testing.T) {
	hookCalls := 0
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 2}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	if _, err := registry.ResolveWithHook("orders", version, version, func(ConflictResolutionEvent) {
		hookCalls++
	}); err != nil {
		t.Fatalf("Resolve(equal) error = %v", err)
	}
	if hookCalls != 0 {
		t.Fatalf("hook calls = %d, want zero for equal versions", hookCalls)
	}
}
