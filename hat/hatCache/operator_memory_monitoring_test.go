package hatCache_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatMetrics"
)

func TestMonitoringPrometheusMetricsExposeOperatorRetainedMemory(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	memory := hatMetrics.NewOperatorMemoryRegistry()
	if err := memory.Set("scan", 1024); err != nil {
		t.Fatalf("Set(scan) error = %v", err)
	}
	if err := memory.Set("sort", 2048); err != nil {
		t.Fatalf("Set(sort) error = %v", err)
	}
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:       "node-a",
		OperatorMemory: memory,
	})

	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", response.Code)
	}
	body := response.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_operator_retained_memory_bytes",
		"# TYPE hatrie_cache_operator_retained_memory_bytes gauge",
		`hatrie_cache_operator_retained_memory_bytes{node="node-a",operator="scan"} 1024`,
		`hatrie_cache_operator_retained_memory_bytes{node="node-a",operator="sort"} 2048`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringOperatorMemoryMetricsAreOptIn(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{NodeName: "node-a"})
	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(response.Body.String(), "hatrie_cache_operator_retained_memory_bytes") {
		t.Fatalf("default metrics unexpectedly expose operator memory:\n%s", response.Body.String())
	}
}
