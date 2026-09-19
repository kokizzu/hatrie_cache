package hatJournal

import (
	"errors"
	"strings"
	"testing"
)

func TestSpaceSyncPolicyRegistryResolvesDefaultsAndOverrides(t *testing.T) {
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{
		Capacity:      4,
		DefaultPolicy: SpaceSyncPolicyPeriodic,
	})
	if err != nil {
		t.Fatalf("NewSpaceSyncPolicyRegistry() error = %v", err)
	}
	if got := registry.Resolve("orders"); got != SpaceSyncPolicyPeriodic {
		t.Fatalf("default policy = %v, want periodic", got)
	}
	if err := registry.Register(" orders ", SpaceSyncPolicyImmediate); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if got := registry.Resolve("orders"); got != SpaceSyncPolicyImmediate {
		t.Fatalf("registered policy = %v, want immediate", got)
	}
	entries := registry.Snapshot()
	if len(entries) != 1 || entries[0].Space != "orders" || entries[0].Policy != SpaceSyncPolicyImmediate {
		t.Fatalf("Snapshot() = %#v, want normalized orders override", entries)
	}
	if !registry.Remove(" orders ") || registry.Remove("orders") {
		t.Fatal("Remove() did not report exactly one existing override")
	}
	if got := registry.Resolve("orders"); got != SpaceSyncPolicyPeriodic {
		t.Fatalf("policy after Remove() = %v, want periodic default", got)
	}
}

func TestSpaceSyncPolicyRegistryValidatesBoundsAndCopiesConfiguration(t *testing.T) {
	if _, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{DefaultPolicy: SpaceSyncPolicy(99)}); !errors.Is(err, ErrSpaceSyncPolicyInvalid) {
		t.Fatalf("invalid default error = %v, want ErrSpaceSyncPolicyInvalid", err)
	}
	if _, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{Capacity: MaxSpaceSyncPolicyEntries + 1}); !errors.Is(err, ErrSpaceSyncPolicyCapacityInvalid) {
		t.Fatalf("invalid capacity error = %v, want ErrSpaceSyncPolicyCapacityInvalid", err)
	}
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(" ", SpaceSyncPolicyImmediate); !errors.Is(err, ErrSpaceSyncPolicySpaceInvalid) {
		t.Fatalf("empty space error = %v, want ErrSpaceSyncPolicySpaceInvalid", err)
	}
	if err := registry.Register(strings.Repeat("x", MaxSpaceSyncPolicySpaceBytes+1), SpaceSyncPolicyImmediate); !errors.Is(err, ErrSpaceSyncPolicySpaceInvalid) {
		t.Fatalf("oversized space error = %v, want ErrSpaceSyncPolicySpaceInvalid", err)
	}
	if err := registry.Register("orders", SpaceSyncPolicy(99)); !errors.Is(err, ErrSpaceSyncPolicyInvalid) {
		t.Fatalf("invalid policy error = %v, want ErrSpaceSyncPolicyInvalid", err)
	}
	if err := registry.Register("orders", SpaceSyncPolicyImmediate); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("users", SpaceSyncPolicyPeriodic); !errors.Is(err, ErrSpaceSyncPolicyCapacityExceeded) {
		t.Fatalf("capacity error = %v, want ErrSpaceSyncPolicyCapacityExceeded", err)
	}
	if got := registry.Resolve(" orders "); got != SpaceSyncPolicyImmediate {
		t.Fatalf("Resolve() did not normalize lookup, got %v", got)
	}
}
