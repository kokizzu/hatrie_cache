package hatMemoryStats

import "testing"

func TestComputeReportBoundsReclaimableRatio(t *testing.T) {
	report := Compute(Values{HeapFreeBytes: 200, TotalBytes: 100})
	if report.ReclaimableRatio != 1 {
		t.Fatalf("reclaimable ratio was not bounded: %+v", report)
	}
}
