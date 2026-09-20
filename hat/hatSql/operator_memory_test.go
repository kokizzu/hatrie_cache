package hatSql

import "testing"

func TestCHG42OperatorMemoryTrackerRecordsPeakAndRelease(t *testing.T) {
	tracker, err := NewSQLOperatorMemoryTracker(128)
	if err != nil {
		t.Fatalf("create tracker: %v", err)
	}
	if err := tracker.Observe("SORT", 64); err != nil {
		t.Fatalf("observe first sample: %v", err)
	}
	if err := tracker.Observe("SORT", 96); err != nil {
		t.Fatalf("observe peak sample: %v", err)
	}
	tracker.Release("SORT")

	snapshot := tracker.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(snapshot))
	}
	if snapshot[0].Operator != "SORT" {
		t.Fatalf("operator = %q, want SORT", snapshot[0].Operator)
	}
	if snapshot[0].CurrentBytes != 0 {
		t.Fatalf("current bytes = %d, want 0 after release", snapshot[0].CurrentBytes)
	}
	if snapshot[0].PeakBytes != 96 {
		t.Fatalf("peak bytes = %d, want 96", snapshot[0].PeakBytes)
	}
	if snapshot[0].Samples != 2 {
		t.Fatalf("samples = %d, want 2", snapshot[0].Samples)
	}
}

func TestCHG42OperatorMemoryTrackerRejectsOverLimit(t *testing.T) {
	tracker, err := NewSQLOperatorMemoryTracker(64)
	if err != nil {
		t.Fatalf("create tracker: %v", err)
	}
	if err := tracker.Observe("GROUP BY", 65); err == nil {
		t.Fatal("expected an operator memory limit error")
	}
	snapshot := tracker.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(snapshot))
	}
	if snapshot[0].Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", snapshot[0].Rejected)
	}
}
