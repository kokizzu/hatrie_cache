package hatMonitoring_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatMonitoring"
)

func TestServerRegistersPublicMonitoringRoute(t *testing.T) {
	server := hatMonitoring.NewServer()
	server.HandleFunc("/api/health", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}
