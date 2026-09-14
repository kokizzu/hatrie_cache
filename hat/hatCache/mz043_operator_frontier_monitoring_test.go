package hatCache_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatMetrics"
)

func TestMonitoringPrometheusMetricsExposeOperatorFrontierLag(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	frontiers := hatMetrics.NewOperatorFrontierRegistry()
	if err := frontiers.Advance("sort", 7); err != nil {
		t.Fatalf("Advance(sort) error = %v", err)
	}
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:                 "node-a",
		OperatorFrontier:         frontiers,
		OperatorFrontierObserved: func() uint64 { return 10 },
	})

	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", response.Code)
	}
	body := response.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_operator_frontier",
		"# TYPE hatrie_cache_operator_frontier gauge",
		`hatrie_cache_operator_frontier{node="node-a",operator="sort"} 7`,
		"# HELP hatrie_cache_operator_observed",
		`hatrie_cache_operator_observed{node="node-a"} 10`,
		"# HELP hatrie_cache_operator_lag",
		`hatrie_cache_operator_lag{node="node-a",operator="sort"} 3`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringOperatorFrontierMetricsAreOptInAndLagNeedsObservedCallback(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	withoutRegistry := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{NodeName: "node-a"})
	response := httptest.NewRecorder()
	withoutRegistry.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(response.Body.String(), "hatrie_cache_operator_frontier") {
		t.Fatalf("default metrics unexpectedly expose operator frontier gauges:\n%s", response.Body.String())
	}

	frontiers := hatMetrics.NewOperatorFrontierRegistry()
	if err := frontiers.Advance("sort", 7); err != nil {
		t.Fatalf("Advance(sort) error = %v", err)
	}
	withoutObserved := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:         "node-a",
		OperatorFrontier: frontiers,
	})
	response = httptest.NewRecorder()
	withoutObserved.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if !strings.Contains(body, `hatrie_cache_operator_frontier{node="node-a",operator="sort"} 7`) {
		t.Fatalf("operator frontier gauge missing without observed callback:\n%s", body)
	}
	for _, metric := range []string{"hatrie_cache_operator_observed", "hatrie_cache_operator_lag"} {
		if strings.Contains(body, metric) {
			t.Fatalf("%s unexpectedly emitted without observed callback:\n%s", metric, body)
		}
	}
}

func TestMonitoringOperatorFrontierEscapesOperatorLabel(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	frontiers := hatMetrics.NewOperatorFrontierRegistry()
	if err := frontiers.Advance("scan\"filter", 7); err != nil {
		t.Fatalf("Advance(quoted operator) error = %v", err)
	}
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:         "node-a",
		OperatorFrontier: frontiers,
	})

	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	want := `hatrie_cache_operator_frontier{node="node-a",operator="scan\"filter"} 7`
	if !strings.Contains(response.Body.String(), want) {
		t.Fatalf("metrics body missing escaped label %q:\n%s", want, response.Body.String())
	}
}
