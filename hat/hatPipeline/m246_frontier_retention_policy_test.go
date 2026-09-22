package hatPipeline

import (
	"errors"
	"testing"
)

func TestM246FrontierRetentionPolicyTracksBoundedUsage(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	for _, id := range []string{"orders", "events"} {
		if err := frontiers.Register(id); err != nil {
			t.Fatalf("Register(%q) error = %v", id, err)
		}
		if err := frontiers.Advance(id, 10, 100); err != nil {
			t.Fatalf("Advance(%q) error = %v", id, err)
		}
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxPolicies: 1})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	defer retention.Close()

	if err := retention.SetPolicy("orders", FrontierRetentionPolicy{MaxHistory: 90, MaxBytes: 4096}); err != nil {
		t.Fatalf("SetPolicy() error = %v", err)
	}
	if err := retention.SetPolicy("orders", FrontierRetentionPolicy{MaxHistory: 90, MaxBytes: 4096}); err != nil {
		t.Fatalf("SetPolicy(replace) error = %v", err)
	}
	if err := retention.SetUsage("orders", FrontierRetentionUsage{RetainedHistory: 91, RetainedBytes: 4097}); err != nil {
		t.Fatalf("SetUsage() error = %v", err)
	}
	snapshot, err := retention.PolicySnapshot("orders")
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !snapshot.PolicyConfigured || snapshot.MaxRetainedHistory != 90 || snapshot.MaxRetainedBytes != 4096 || snapshot.RetainedHistory != 91 || snapshot.RetainedBytes != 4097 || !snapshot.HistoryBudgetExceeded || !snapshot.BytesBudgetExceeded || !snapshot.BudgetExceeded {
		t.Fatalf("Snapshot(over budget) = %#v", snapshot)
	}

	if err := retention.SetUsage("orders", FrontierRetentionUsage{RetainedHistory: 90, RetainedBytes: 4096}); err != nil {
		t.Fatalf("SetUsage(within budget) error = %v", err)
	}
	snapshot, err = retention.PolicySnapshot("orders")
	if err != nil {
		t.Fatalf("Snapshot(within budget) error = %v", err)
	}
	if snapshot.HistoryBudgetExceeded || snapshot.BytesBudgetExceeded || snapshot.BudgetExceeded {
		t.Fatalf("Snapshot(within budget) = %#v", snapshot)
	}

	all := retention.SnapshotAll()
	if len(all) != 2 || all[0].FrontierID != "events" || all[1].FrontierID != "orders" {
		t.Fatalf("SnapshotAll() = %#v", all)
	}
	policies := retention.PolicySnapshots()
	if len(policies) != 1 || policies[0].FrontierID != "orders" || !policies[0].PolicyConfigured {
		t.Fatalf("PolicySnapshots() = %#v", policies)
	}

	if err := retention.ClearPolicy("orders"); err != nil {
		t.Fatalf("ClearPolicy() error = %v", err)
	}
	snapshot, err = retention.PolicySnapshot("orders")
	if err != nil {
		t.Fatalf("Snapshot(after clear) error = %v", err)
	}
	if snapshot.PolicyConfigured || snapshot.MaxRetainedHistory != 0 || snapshot.MaxRetainedBytes != 0 || snapshot.RetainedHistory != 0 || snapshot.RetainedBytes != 0 {
		t.Fatalf("Snapshot(after clear) = %#v", snapshot)
	}
}

func TestM246FrontierRetentionPolicyBoundsAndErrors(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	for _, id := range []string{"orders", "events"} {
		if err := frontiers.Register(id); err != nil {
			t.Fatalf("Register(%q) error = %v", id, err)
		}
		if err := frontiers.Advance(id, 1, 10); err != nil {
			t.Fatalf("Advance(%q) error = %v", id, err)
		}
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxPolicies: 1})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	if err := retention.SetPolicy("orders", FrontierRetentionPolicy{}); !errors.Is(err, ErrFrontierRetentionPolicyInvalid) {
		t.Fatalf("SetPolicy(zero) error = %v, want %v", err, ErrFrontierRetentionPolicyInvalid)
	}
	if err := retention.SetUsage("orders", FrontierRetentionUsage{RetainedHistory: 1}); !errors.Is(err, ErrFrontierRetentionPolicyNotFound) {
		t.Fatalf("SetUsage(without policy) error = %v, want %v", err, ErrFrontierRetentionPolicyNotFound)
	}
	if err := retention.SetPolicy("orders", FrontierRetentionPolicy{MaxHistory: 1}); err != nil {
		t.Fatalf("SetPolicy(orders) error = %v", err)
	}
	if err := retention.SetPolicy("events", FrontierRetentionPolicy{MaxBytes: 1}); !errors.Is(err, ErrFrontierRetentionPolicyLimit) {
		t.Fatalf("SetPolicy(over limit) error = %v, want %v", err, ErrFrontierRetentionPolicyLimit)
	}
	if err := retention.ClearPolicy("orders"); err != nil {
		t.Fatalf("ClearPolicy(orders) error = %v", err)
	}
	if err := retention.SetPolicy("events", FrontierRetentionPolicy{MaxBytes: 1}); err != nil {
		t.Fatalf("SetPolicy(events after clear) error = %v", err)
	}
	if err := retention.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := retention.SetPolicy("events", FrontierRetentionPolicy{MaxBytes: 1}); !errors.Is(err, ErrFrontierRetentionClosed) {
		t.Fatalf("SetPolicy(after close) error = %v, want %v", err, ErrFrontierRetentionClosed)
	}
}
