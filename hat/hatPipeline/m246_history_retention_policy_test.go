package hatPipeline

import (
	"errors"
	"testing"
)

func TestM246RetentionPolicyBoundsAgeAndStorage(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := frontiers.Advance("orders", 10, 100); err != nil {
		t.Fatal(err)
	}
	registry, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.SetPolicy("orders", FrontierRetentionPolicy{MaxAge: 10, MaxBytes: 1024}); err != nil {
		t.Fatal(err)
	}
	if err := registry.ObserveStorage("orders", 512); err != nil {
		t.Fatal(err)
	}
	safe, err := registry.SafeCompactionBefore("orders")
	if err != nil {
		t.Fatal(err)
	}
	if safe != 10 {
		t.Fatalf("safe compaction boundary without leases = %d, want 10", safe)
	}

	lease, err := registry.Acquire("orders", 95)
	if err != nil {
		t.Fatalf("within-policy Acquire() error = %v", err)
	}
	defer registry.Release(lease)
	if _, err := registry.Acquire("orders", 80); !errors.Is(err, ErrFrontierRetentionPolicyViolation) {
		t.Fatalf("old Acquire() error = %v, want ErrFrontierRetentionPolicyViolation", err)
	}
	if err := registry.ObserveStorage("orders", 2048); !errors.Is(err, ErrFrontierRetentionPolicyViolation) {
		t.Fatalf("oversized storage error = %v, want ErrFrontierRetentionPolicyViolation", err)
	}

	snapshot, err := registry.Snapshot("orders")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StoredBytes != 512 || snapshot.Policy.MaxBytes != 1024 || snapshot.Policy.MaxAge != 10 {
		t.Fatalf("snapshot policy/storage = %#v, want 512 bytes and policy 10/1024", snapshot)
	}
}

func TestM246RetentionPolicyBoundaryAndClear(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, frontierID := range []string{"orders", "users"} {
		if err := frontiers.Register(frontierID); err != nil {
			t.Fatal(err)
		}
		if err := frontiers.Advance(frontierID, 0, 100); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.SetPolicy("orders", FrontierRetentionPolicy{MaxAge: 10, MaxBytes: 100}); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetPolicy("users", FrontierRetentionPolicy{MaxAge: 50, MaxBytes: 1000}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("orders", 90); err != nil {
		t.Fatalf("exact age boundary Acquire() error = %v", err)
	}
	if _, err := registry.Acquire("orders", 89); !errors.Is(err, ErrFrontierRetentionPolicyViolation) {
		t.Fatalf("one tick beyond age boundary error = %v", err)
	}
	if _, err := registry.Acquire("users", 60); err != nil {
		t.Fatalf("independent policy Acquire() error = %v", err)
	}
	if err := registry.ObserveStorage("orders", 100); err != nil {
		t.Fatal(err)
	}
	if err := registry.ObserveStorage("orders", 101); !errors.Is(err, ErrFrontierRetentionPolicyViolation) {
		t.Fatalf("oversized update error = %v", err)
	}
	snapshot, err := registry.Snapshot("orders")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StoredBytes != 100 {
		t.Fatalf("stored bytes after rejected update = %d, want 100", snapshot.StoredBytes)
	}
	if err := registry.ClearPolicy("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("orders", 89); err != nil {
		t.Fatalf("Acquire() after ClearPolicy error = %v", err)
	}
	snapshot, err = registry.Snapshot("orders")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Policy != (FrontierRetentionPolicy{}) || snapshot.StoredBytes != 100 {
		t.Fatalf("snapshot after ClearPolicy = %#v, want cleared policy and 100 bytes", snapshot)
	}
}
