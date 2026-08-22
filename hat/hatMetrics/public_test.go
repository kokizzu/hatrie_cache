package hatMetrics_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestAPIMetricsIsUsableByImporters(t *testing.T) {
	metrics := hatMetrics.NewAPIMetrics()
	metrics.RecordAuditResult(nil)
	metrics.RecordAuditResult(errors.New("write failed"))
	metrics.RecordWriteProtectionRejection()
	metrics.RecordRateLimitRejection()

	if got := metrics.Snapshot(); got.AuditEventsTotal != 2 || got.AuditErrorsTotal != 1 || got.WriteProtectionRejectionsTotal != 1 || got.RateLimitRejectionsTotal != 1 {
		t.Fatalf("Snapshot() = %#v, want recorded totals", got)
	}
}
