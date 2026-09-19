package hatMetrics_test

import (
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestT044HeapFragmentationReportIsUsableByImporters(t *testing.T) {
	report := hatMetrics.ReadHeapFragmentationReport()
	if report.HeapReleasedBytes > report.HeapIdleBytes {
		t.Fatalf("HeapReleasedBytes = %d, HeapIdleBytes = %d", report.HeapReleasedBytes, report.HeapIdleBytes)
	}
}
