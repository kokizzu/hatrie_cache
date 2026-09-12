package hatPipeline

import (
	"errors"
	"testing"
)

func TestFrontierRetentionTracksSafeCompactionBoundary(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 4})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("orders"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("orders", 10, 20); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxLeases: 4})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	if got, err := retention.SafeCompactionBefore("orders"); err != nil || got != 10 {
		t.Fatalf("SafeCompactionBefore(without lease) = %d, %v; want 10", got, err)
	}
	if _, err := retention.Acquire("orders", 9); !errors.Is(err, ErrFrontierRetentionExpired) {
		t.Fatalf("Acquire(expired) error = %v, want %v", err, ErrFrontierRetentionExpired)
	}
	if _, err := retention.Acquire("orders", 21); !errors.Is(err, ErrFrontierRetentionAhead) {
		t.Fatalf("Acquire(ahead) error = %v, want %v", err, ErrFrontierRetentionAhead)
	}
	first, err := retention.Acquire("orders", 12)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	second, err := retention.Acquire("orders", 15)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}
	if err := frontiers.Advance("orders", 16, 20); err != nil {
		t.Fatalf("Advance(after leases) error = %v", err)
	}
	if got, err := retention.SafeCompactionBefore("orders"); err != nil || got != 12 {
		t.Fatalf("SafeCompactionBefore(two leases) = %d, %v; want 12", got, err)
	}
	if err := retention.Release(first); err != nil {
		t.Fatalf("Release(first) error = %v", err)
	}
	if got, err := retention.SafeCompactionBefore("orders"); err != nil || got != 15 {
		t.Fatalf("SafeCompactionBefore(one lease) = %d, %v; want 15", got, err)
	}
	if err := retention.Release(second); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
	if got, err := retention.SafeCompactionBefore("orders"); err != nil || got != 16 {
		t.Fatalf("SafeCompactionBefore(after release) = %d, %v; want 16", got, err)
	}
}

func TestFrontierRetentionBoundsLeasesAndCloses(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("events"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("events", 1, 1); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{MaxLeases: 1})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	lease, err := retention.Acquire("events", 1)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if _, err := retention.Acquire("events", 1); !errors.Is(err, ErrFrontierRetentionLeaseLimit) {
		t.Fatalf("Acquire(over limit) error = %v, want %v", err, ErrFrontierRetentionLeaseLimit)
	}
	if err := retention.Release(lease); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := retention.Release(lease); !errors.Is(err, ErrFrontierRetentionLeaseNotFound) {
		t.Fatalf("Release(repeated) error = %v, want %v", err, ErrFrontierRetentionLeaseNotFound)
	}
	if err := retention.Release(FrontierRetentionLease{}); !errors.Is(err, ErrFrontierRetentionLeaseInvalid) {
		t.Fatalf("Release(invalid) error = %v, want %v", err, ErrFrontierRetentionLeaseInvalid)
	}
	if err := retention.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := retention.Acquire("events", 1); !errors.Is(err, ErrFrontierRetentionClosed) {
		t.Fatalf("Acquire(after close) error = %v, want %v", err, ErrFrontierRetentionClosed)
	}
	if _, err := retention.SafeCompactionBefore("events"); !errors.Is(err, ErrFrontierRetentionClosed) {
		t.Fatalf("SafeCompactionBefore(after close) error = %v, want %v", err, ErrFrontierRetentionClosed)
	}
}

func TestFrontierRetentionSnapshotsAndContext(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	for _, id := range []string{"zeta", "alpha"} {
		if err := frontiers.Register(id); err != nil {
			t.Fatalf("Register(%q) error = %v", id, err)
		}
		if err := frontiers.Advance(id, 5, 10); err != nil {
			t.Fatalf("Advance(%q) error = %v", id, err)
		}
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	lease, err := retention.Acquire("zeta", 7)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if lease.ID == 0 {
		t.Fatal("Acquire() returned zero lease ID")
	}
	all := retention.SnapshotAll()
	if len(all) != 2 || all[0].FrontierID != "alpha" || all[1].FrontierID != "zeta" || all[1].LeaseCount != 1 || all[1].MinimumRequired != 7 {
		t.Fatalf("SnapshotAll() = %#v", all)
	}
}

func TestFrontierRetentionRejectsClosedFrontiers(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("events"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	if err := frontiers.Close(); err != nil {
		t.Fatalf("frontiers.Close() error = %v", err)
	}
	if _, err := retention.Acquire("events", 0); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("Acquire(closed frontier) error = %v, want %v", err, ErrFrontierClosed)
	}
	if _, err := retention.SafeCompactionBefore("events"); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("SafeCompactionBefore(closed frontier) error = %v, want %v", err, ErrFrontierClosed)
	}
}
