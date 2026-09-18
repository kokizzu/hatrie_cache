package hatSql

import (
	"context"
	"testing"
)

func newM032FrontierBarrier(t *testing.T, source string) *SQLSourceFrontierBarrier {
	t.Helper()
	barrier, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{
		{Source: source, Partition: "a"},
		{Source: source, Partition: "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return barrier
}

func TestSQLDistributedFrontierCoordinatorCombinesIndependentBarriers(t *testing.T) {
	left := newM032FrontierBarrier(t, "left")
	right := newM032FrontierBarrier(t, "right")
	coordinator, err := NewSQLDistributedFrontierCoordinator([]*SQLSourceFrontierBarrier{left, right})
	if err != nil {
		t.Fatal(err)
	}
	if frontier, ready := coordinator.CommonFrontier(); frontier != 0 || ready {
		t.Fatalf("initial CommonFrontier() = %d/%t, want 0/false", frontier, ready)
	}
	if _, err := left.ObserveBatch([]SQLSourceFrontier{{Source: "left", Partition: "a", Frontier: 10}, {Source: "left", Partition: "b", Frontier: 10}}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "a", Frontier: 10}); err != nil {
		t.Fatal(err)
	}
	if frontier, ready := coordinator.CommonFrontier(); frontier != 0 || ready {
		t.Fatalf("unobserved CommonFrontier() = %d/%t, want 0/false", frontier, ready)
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "b", Frontier: 9}); err != nil {
		t.Fatal(err)
	}
	if frontier, ready := coordinator.CommonFrontier(); frontier != 9 || !ready {
		t.Fatalf("divergent CommonFrontier() = %d/%t, want 9/true", frontier, ready)
	}
	if !coordinator.ReadyAt(9) || coordinator.ReadyAt(10) {
		t.Fatal("ReadyAt() did not combine all independent barriers")
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "b", Frontier: 10}); err != nil {
		t.Fatal(err)
	}
	if frontier, ready := coordinator.CommonFrontier(); frontier != 10 || !ready {
		t.Fatalf("ready CommonFrontier() = %d/%t, want 10/true", frontier, ready)
	}
	if _, err := left.ObserveBatch([]SQLSourceFrontier{{Source: "left", Partition: "a", Frontier: 11}, {Source: "left", Partition: "b", Frontier: 11}}); err != nil {
		t.Fatal(err)
	}
	if frontier, ready := coordinator.CommonFrontier(); frontier != 10 || !ready {
		t.Fatalf("divergent CommonFrontier() = %d/%t, want 10/true", frontier, ready)
	}
	provider := &m032SnapshotProvider{}
	view, release, err := BeginSQLDistributedFrontierSnapshot(context.Background(), provider, coordinator, 10)
	if err != nil {
		t.Fatal(err)
	}
	if view == nil || provider.frontier != 10 {
		t.Fatalf("distributed snapshot = %v at frontier %d, want non-nil at 10", view, provider.frontier)
	}
	release()
}

type m032SnapshotProvider struct {
	frontier uint64
}

func (provider *m032SnapshotProvider) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (provider *m032SnapshotProvider) BeginSQLSnapshotAt(_ context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	provider.frontier = frontier
	return provider, func() {}, nil
}
