package hatReplication

import (
	"errors"
	"testing"
)

func TestConflictPolicyRegistryAppliesPerSpacePolicy(t *testing.T) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		t.Fatal(err)
	}
	old := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	newer := ConflictVersion{Timestamp: 2, NodeID: "node-z", Sequence: 1}
	winner, err := registry.Resolve("default", old, newer)
	if err != nil || winner != newer {
		t.Fatalf("default Resolve() = %#v/%v, want newer version", winner, err)
	}

	if err := registry.Set("payments", ConflictPolicy{
		Mode:           ConflictPolicySourcePriority,
		SourcePriority: []string{"node-a", "node-z"},
	}); err != nil {
		t.Fatal(err)
	}
	priorityWinner, err := registry.Resolve("payments", newer, old)
	if err != nil || priorityWinner != old {
		t.Fatalf("priority Resolve() = %#v/%v, want node-a version", priorityWinner, err)
	}

	if err := registry.Set("strict", ConflictPolicy{Mode: ConflictPolicyReject}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("strict", old, newer); !errors.Is(err, ErrConflictRejected) {
		t.Fatalf("reject Resolve() error = %v, want ErrConflictRejected", err)
	}
	if winner, err := registry.Resolve("strict", old, old); err != nil || winner != old {
		t.Fatalf("equal reject Resolve() = %#v/%v, want equal version", winner, err)
	}
	if !registry.Delete("strict") || registry.Delete("strict") {
		t.Fatal("Delete() did not report one removal")
	}
}

func TestConflictPolicyRegistryValidatesAndCopiesPriority(t *testing.T) {
	priority := []string{"node-a", "node-b"}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicySourcePriority, SourcePriority: priority})
	if err != nil {
		t.Fatal(err)
	}
	priority[0] = "node-z"
	defaultWinner, err := registry.Resolve("default", ConflictVersion{Timestamp: 1, NodeID: "node-a"}, ConflictVersion{Timestamp: 9, NodeID: "node-z"})
	if err != nil || defaultWinner.NodeID != "node-a" {
		t.Fatalf("copied default priority Resolve() = %#v/%v, want node-a", defaultWinner, err)
	}
	if err := registry.Set("orders", ConflictPolicy{Mode: ConflictPolicySourcePriority, SourcePriority: []string{"node-a"}}); err != nil {
		t.Fatal(err)
	}
	winner, err := registry.Resolve("orders", ConflictVersion{Timestamp: 9, NodeID: "node-z"}, ConflictVersion{Timestamp: 1, NodeID: "node-a"})
	if err != nil || winner.NodeID != "node-a" {
		t.Fatalf("copied priority Resolve() = %#v/%v, want node-a", winner, err)
	}

	for _, policy := range []ConflictPolicy{
		{Mode: ConflictPolicyMode(99)},
		{Mode: ConflictPolicySourcePriority},
		{Mode: ConflictPolicySourcePriority, SourcePriority: []string{"node-a", "node-a"}},
	} {
		if err := registry.Set("invalid", policy); !errors.Is(err, ErrConflictPolicyInvalid) {
			t.Fatalf("Set(%#v) error = %v, want ErrConflictPolicyInvalid", policy, err)
		}
	}
	if err := registry.Set("", ConflictPolicy{Mode: ConflictPolicyLastWriteWins}); !errors.Is(err, ErrConflictPolicySpaceRequired) {
		t.Fatalf("Set(empty) error = %v, want ErrConflictPolicySpaceRequired", err)
	}
}
