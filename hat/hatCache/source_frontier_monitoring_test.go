package hatCache_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatMetrics"
)

func TestMonitoringPrometheusMetricsExposeSourceFrontierLag(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	frontiers := hatMetrics.NewSourceFrontierRegistry()
	if err := frontiers.Advance("orders", 7); err != nil {
		t.Fatalf("Advance(orders) error = %v", err)
	}
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:               "node-a",
		SourceFrontier:         frontiers,
		SourceFrontierObserved: func() uint64 { return 10 },
	})

	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", response.Code)
	}
	body := response.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_source_frontier",
		"# TYPE hatrie_cache_source_frontier gauge",
		`hatrie_cache_source_frontier{node="node-a",source="orders"} 7`,
		"# HELP hatrie_cache_source_observed",
		`hatrie_cache_source_observed{node="node-a"} 10`,
		"# HELP hatrie_cache_source_lag",
		`hatrie_cache_source_lag{node="node-a",source="orders"} 3`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringSourceFrontierMetricsAreOptInAndLagNeedsObservedCallback(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	withoutRegistry := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{NodeName: "node-a"})
	response := httptest.NewRecorder()
	withoutRegistry.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(response.Body.String(), "hatrie_cache_source_frontier") {
		t.Fatalf("default metrics unexpectedly expose source frontier gauges:\n%s", response.Body.String())
	}

	frontiers := hatMetrics.NewSourceFrontierRegistry()
	if err := frontiers.Advance("orders", 7); err != nil {
		t.Fatalf("Advance(orders) error = %v", err)
	}
	withoutObserved := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:       "node-a",
		SourceFrontier: frontiers,
	})
	response = httptest.NewRecorder()
	withoutObserved.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if !strings.Contains(body, `hatrie_cache_source_frontier{node="node-a",source="orders"} 7`) {
		t.Fatalf("frontier gauge missing without observed callback:\n%s", body)
	}
	for _, metric := range []string{"hatrie_cache_source_observed", "hatrie_cache_source_lag"} {
		if strings.Contains(body, metric) {
			t.Fatalf("%s unexpectedly emitted without observed callback:\n%s", metric, body)
		}
	}
}
