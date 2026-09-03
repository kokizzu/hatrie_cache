package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatMonitoring"
)

func TestMonitoringMemoryEndpointReportsRuntimeDetails(t *testing.T) {
	trie := newTestTrie(t)
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/memory", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/memory status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET /api/memory Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
	}
	var report hatMonitoring.MemoryReport
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode GET /api/memory response: %v", err)
	}
	if report.CollectedAt.IsZero() || report.HeapObjects == 0 || report.GOMemLimitBytes == 0 {
		t.Fatalf("GET /api/memory report = %#v, want timestamp, live heap objects, and memory limit", report)
	}
}

func TestMonitoringMemoryEndpointRejectsUnsupportedMethod(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/memory", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/memory status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestMonitoringMemoryEndpointRequiresConfiguredAuth(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{AuthToken: "current"}).Handler()
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/memory", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized GET /api/memory status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/memory", nil)
	request.Header.Set("X-Hatrie-Auth-Token", "current")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized GET /api/memory status = %d, want %d: %s", authorized.Code, http.StatusOK, authorized.Body.String())
	}
}

func TestMonitoringOpenAPIAdvertisesMemoryEndpoint(t *testing.T) {
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
	if _, ok := document.Paths["/api/memory"]; !ok {
		t.Fatalf("OpenAPI paths = %#v, want /api/memory", document.Paths)
	}
}
