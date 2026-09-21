package hatSql

import "testing"

func TestM209SQLSourceFrontierTrackerKeepsMonotonePartitionState(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 12}); err != nil || !changed {
		t.Fatalf("initial Observe() = %v/%v, want true/nil", changed, err)
	}
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 11}); err != nil || changed {
		t.Fatalf("regressed Observe() = %v/%v, want false/nil", changed, err)
	}
	if frontier, ready := tracker.CommonFrontier(); !ready || frontier != 12 {
		t.Fatalf("CommonFrontier() = %d/%v, want 12/true", frontier, ready)
	}
}
