package hatCache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkT242AuditAllOperationsBaseline(b *testing.B) {
	handler := NewMonitoringHandler(nil, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
	}
}

func BenchmarkT242AuditAllOperationsEnabled(b *testing.B) {
	logger := NewAuditLogger(io.Discard)
	handler := NewMonitoringHandler(nil, MonitoringOptions{
		AuditLog:           logger,
		AuditAllOperations: true,
	}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
	}
}
