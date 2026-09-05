package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMonitoringSchedulerEndpointReportsRuntimeDetails(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/scheduler", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/scheduler status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET /api/scheduler Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	var report MonitoringSchedulerReport
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode GET /api/scheduler response: %v", err)
	}
	if report.CollectedAt.IsZero() || report.Goroutines == 0 || report.GOMAXPROCS == 0 || report.NumCPU == 0 {
		t.Fatalf("GET /api/scheduler report = %#v, want timestamp and positive runtime counts", report)
	}
}

func TestMonitoringSchedulerEndpointRejectsUnsupportedMethod(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/scheduler", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/scheduler status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestMonitoringSchedulerEndpointRequiresConfiguredAuth(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{AuthToken: "scheduler-secret"}).Handler()
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/scheduler", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized GET /api/scheduler status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/scheduler", nil)
	request.Header.Set("X-Hatrie-Auth-Token", "scheduler-secret")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized GET /api/scheduler status = %d, want %d: %s", authorized.Code, http.StatusOK, authorized.Body.String())
	}
}

func TestMonitoringOpenAPIAdvertisesSchedulerEndpoint(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json status = %d, want %d", response.Code, http.StatusOK)
	}
	var document struct {
		Paths map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode OpenAPI document: %v", err)
	}
	if _, ok := document.Paths["/api/scheduler"]; !ok {
		t.Fatalf("OpenAPI paths = %#v, want /api/scheduler", document.Paths)
	}
}

func TestMonitoringPrometheusMetricsExposeSchedulerState(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	for _, metric := range []string{
		"hatrie_cache_goroutines{node=",
		"hatrie_cache_gomaxprocs{node=",
		"hatrie_cache_num_cpu{node=",
	} {
		if !strings.Contains(response.Body.String(), metric) {
			t.Fatalf("metrics body missing %q:\n%s", metric, response.Body.String())
		}
	}
}
