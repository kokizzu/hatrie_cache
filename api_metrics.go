package hatriecache

import "sync/atomic"

type APIMetrics struct {
	auditEventsTotal               atomic.Uint64
	auditErrorsTotal               atomic.Uint64
	writeProtectionRejectionsTotal atomic.Uint64
	rateLimitRejectionsTotal       atomic.Uint64
}

type APIMetricsSnapshot struct {
	AuditEventsTotal               uint64
	AuditErrorsTotal               uint64
	WriteProtectionRejectionsTotal uint64
	RateLimitRejectionsTotal       uint64
}

func NewAPIMetrics() *APIMetrics {
	return &APIMetrics{}
}

func (metrics *APIMetrics) RecordAuditResult(err error) {
	if metrics == nil {
		return
	}
	metrics.auditEventsTotal.Add(1)
	if err != nil {
		metrics.auditErrorsTotal.Add(1)
	}
}

func (metrics *APIMetrics) RecordWriteProtectionRejection() {
	if metrics == nil {
		return
	}
	metrics.writeProtectionRejectionsTotal.Add(1)
}

func (metrics *APIMetrics) RecordRateLimitRejection() {
	if metrics == nil {
		return
	}
	metrics.rateLimitRejectionsTotal.Add(1)
}

func (metrics *APIMetrics) Snapshot() APIMetricsSnapshot {
	if metrics == nil {
		return APIMetricsSnapshot{}
	}
	return APIMetricsSnapshot{
		AuditEventsTotal:               metrics.auditEventsTotal.Load(),
		AuditErrorsTotal:               metrics.auditErrorsTotal.Load(),
		WriteProtectionRejectionsTotal: metrics.writeProtectionRejectionsTotal.Load(),
		RateLimitRejectionsTotal:       metrics.rateLimitRejectionsTotal.Load(),
	}
}
