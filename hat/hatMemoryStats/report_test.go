package hatMemoryStats

import (
	"math"
	"testing"
)

func TestComputeReport(t *testing.T) {
	report := Compute(Values{
		HeapObjectsBytes:  600,
		HeapFreeBytes:     300,
		HeapReleasedBytes: 100,
		HeapStacksBytes:   50,
		TotalBytes:        1200,
	})
	if report.HeapReservedBytes != 900 || report.ReclaimableBytes != 200 {
		t.Fatalf("byte totals = %+v", report)
	}
	if math.Abs(report.FragmentationRatio-1.0/3.0) > 0.0001 {
		t.Fatalf("FragmentationRatio = %f, want 1/3", report.FragmentationRatio)
	}
	if math.Abs(report.ReclaimableRatio-1.0/6.0) > 0.0001 {
		t.Fatalf("ReclaimableRatio = %f, want 1/6", report.ReclaimableRatio)
	}
}

func TestComputeReportHandlesZeroAndInconsistentValues(t *testing.T) {
	if report := Compute(Values{}); report.FragmentationRatio != 0 || report.ReclaimableRatio != 0 {
		t.Fatalf("zero report = %+v", report)
	}
	report := Compute(Values{HeapObjectsBytes: 100, HeapFreeBytes: 50, HeapReleasedBytes: 100})
	if report.HeapReservedBytes != 150 || report.ReclaimableBytes != 0 {
		t.Fatalf("inconsistent report = %+v", report)
	}
}

func TestSnapshotReturnsNonNegativeRuntimeValues(t *testing.T) {
	report := Snapshot()
	if report.HeapReservedBytes < report.HeapObjectsBytes || report.HeapReservedBytes < report.HeapFreeBytes {
		t.Fatalf("runtime report = %+v", report)
	}
	if report.FragmentationRatio < 0 || report.FragmentationRatio > 1 || report.ReclaimableRatio < 0 || report.ReclaimableRatio > 1 {
		t.Fatalf("runtime ratios = %+v", report)
	}
}
