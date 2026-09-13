package hatPipeline_test

import (
	"errors"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestMZ027ActiveLeasesAreOrderedAndDetached(t *testing.T) {
	frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	defer func() { _ = frontiers.Close() }()
	if err := frontiers.Register("events"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("events", 100, 130); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}

	retention, err := hatPipeline.NewFrontierRetentionRegistry(frontiers, hatPipeline.FrontierRetentionOptions{MaxLeases: 4})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	first, err := retention.Acquire("events", 100)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	second, err := retention.Acquire("events", 120)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}

	leases, err := retention.ActiveLeases("events")
	if err != nil {
		t.Fatalf("ActiveLeases() error = %v", err)
	}
	if len(leases) != 2 {
		t.Fatalf("ActiveLeases() length = %d, want 2", len(leases))
	}
	if leases[0] != first || leases[1] != second {
		t.Fatalf("ActiveLeases() = %#v, want [%#v %#v]", leases, first, second)
	}

	leases[0].AsOf = 999
	refetched, err := retention.ActiveLeases("events")
	if err != nil {
		t.Fatalf("ActiveLeases() refetch error = %v", err)
	}
	if refetched[0] != first {
		t.Fatalf("ActiveLeases() returned mutable registry state = %#v, want %#v", refetched[0], first)
	}
	if err := retention.Release(first); err != nil {
		t.Fatalf("Release(first) error = %v", err)
	}
	if err := retention.Release(second); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
	empty, err := retention.ActiveLeases("events")
	if err != nil {
		t.Fatalf("ActiveLeases(empty) error = %v", err)
	}
	if empty != nil {
		t.Fatalf("ActiveLeases(empty) = %#v, want nil", empty)
	}

	if _, err := retention.ActiveLeases(""); !errors.Is(err, hatPipeline.ErrFrontierIDEmpty) {
		t.Fatalf("ActiveLeases(empty) error = %v, want %v", err, hatPipeline.ErrFrontierIDEmpty)
	}
	if err := retention.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := retention.ActiveLeases("events"); !errors.Is(err, hatPipeline.ErrFrontierRetentionClosed) {
		t.Fatalf("ActiveLeases(after close) error = %v, want %v", err, hatPipeline.ErrFrontierRetentionClosed)
	}
}

func ExampleFrontierRetentionRegistry_ActiveLeases() {
	frontiers, _ := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
	defer func() { _ = frontiers.Close() }()
	_ = frontiers.Register("events")
	_ = frontiers.Advance("events", 100, 130)

	retention, _ := hatPipeline.NewFrontierRetentionRegistry(
		frontiers,
		hatPipeline.FrontierRetentionOptions{},
	)
	defer func() { _ = retention.Close() }()
	lease, _ := retention.Acquire("events", 100)
	defer func() { _ = retention.Release(lease) }()

	leases, _ := retention.ActiveLeases("events")
	fmt.Println(leases[0].ID, leases[0].AsOf)
	// Output: 1 100
}
