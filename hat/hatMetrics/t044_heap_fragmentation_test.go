package hatMetrics

import (
	"runtime"
	"testing"
)

func TestT044HeapFragmentationReportDerivesReusableAndMetadataBytes(t *testing.T) {
	report := heapFragmentationReportFromMemStats(runtime.MemStats{
		HeapSys:      1000,
		HeapInuse:    600,
		HeapIdle:     400,
		HeapReleased: 150,
		MSpanInuse:   10,
		MCacheInuse:  20,
		BuckHashSys:  30,
		GCSys:        40,
		OtherSys:     50,
		StackInuse:   60,
		StackSys:     70,
	})

	if report.HeapSysBytes != 1000 || report.HeapInuseBytes != 600 || report.HeapIdleBytes != 400 || report.HeapReleasedBytes != 150 {
		t.Fatalf("heap fields = %#v, want runtime values", report)
	}
	if report.ReusableBytes != 250 {
		t.Fatalf("ReusableBytes = %d, want 250", report.ReusableBytes)
	}
	if report.MetadataBytes != 150 {
		t.Fatalf("MetadataBytes = %d, want 150", report.MetadataBytes)
	}
	if report.StackInuseBytes != 60 || report.StackSysBytes != 70 {
		t.Fatalf("stack fields = %#v, want inuse=60/sys=70", report)
	}
}

func TestT044HeapFragmentationReportClampsInconsistentRuntimeValues(t *testing.T) {
	report := heapFragmentationReportFromMemStats(runtime.MemStats{
		HeapIdle:     10,
		HeapReleased: 20,
		MSpanInuse:   ^uint64(0),
		MCacheInuse:  1,
	})

	if report.ReusableBytes != 0 {
		t.Fatalf("ReusableBytes = %d, want clamp to zero", report.ReusableBytes)
	}
	if report.MetadataBytes != ^uint64(0) {
		t.Fatalf("MetadataBytes = %d, want saturating sum", report.MetadataBytes)
	}
}

func TestT044ReadHeapFragmentationReport(t *testing.T) {
	report := ReadHeapFragmentationReport()
	if report.HeapInuseBytes > report.HeapSysBytes {
		t.Fatalf("heap in-use bytes %d exceed heap sys bytes %d", report.HeapInuseBytes, report.HeapSysBytes)
	}
	if report.HeapReleasedBytes > report.HeapIdleBytes {
		t.Fatalf("heap released bytes %d exceed heap idle bytes %d", report.HeapReleasedBytes, report.HeapIdleBytes)
	}
}
