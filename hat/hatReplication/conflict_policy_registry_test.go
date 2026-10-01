package hatReplication

import (
	"errors"
	"sync"
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

func TestConflictPolicyRegistrySnapshotTracksGenerationAndCopiesPolicy(t *testing.T) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		t.Fatal(err)
	}

	initial, err := registry.Snapshot("payments")
	if err != nil {
		t.Fatal(err)
	}
	if initial.Space != "payments" || initial.Policy.Mode != ConflictPolicyLastWriteWins || initial.Overridden || initial.Generation != 0 {
		t.Fatalf("initial Snapshot() = %#v, want default policy without override at generation 0", initial)
	}

	if err := registry.Set("payments", ConflictPolicy{
		Mode:           ConflictPolicySourcePriority,
		SourcePriority: []string{"node-a", "node-b"},
	}); err != nil {
		t.Fatal(err)
	}
	installed, err := registry.Snapshot("payments")
	if err != nil {
		t.Fatal(err)
	}
	if installed.Space != "payments" || installed.Policy.Mode != ConflictPolicySourcePriority || !installed.Overridden || installed.Generation <= initial.Generation {
		t.Fatalf("installed Snapshot() = %#v, want updated override generation", installed)
	}
	installed.Policy.SourcePriority[0] = "mutated"

	unchanged, err := registry.Snapshot("payments")
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Policy.SourcePriority[0] != "node-a" {
		t.Fatalf("Snapshot() exposed mutable policy storage: %#v", unchanged.Policy.SourcePriority)
	}

	if !registry.Delete("payments") {
		t.Fatal("Delete(payments) = false, want true")
	}
	removed, err := registry.Snapshot("payments")
	if err != nil {
		t.Fatal(err)
	}
	if removed.Overridden || removed.Policy.Mode != ConflictPolicyLastWriteWins || removed.Generation <= unchanged.Generation {
		t.Fatalf("removed Snapshot() = %#v, want default policy at a newer generation", removed)
	}
}

func TestConflictPolicyRegistryConcurrentReadersAndWriters(t *testing.T) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}

	var group sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 200; iteration++ {
				if _, err := registry.Resolve("payments", left, right); err != nil {
					t.Errorf("Resolve() error = %v", err)
				}
				if snapshot, err := registry.Snapshot("payments"); err != nil || snapshot.Space != "payments" {
					t.Errorf("Snapshot() = %#v/%v", snapshot, err)
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for iteration := 0; iteration < 200; iteration++ {
			if err := registry.Set("payments", ConflictPolicy{
				Mode:           ConflictPolicySourcePriority,
				SourcePriority: []string{"node-a", "node-b"},
			}); err != nil {
				t.Errorf("Set() error = %v", err)
			}
			if !registry.Delete("payments") {
				t.Errorf("Delete() = false, want true")
			}
		}
	}()
	group.Wait()
}
