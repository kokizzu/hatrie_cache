package hatCache_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatMetrics"
)

func BenchmarkMonitoringMetricsWithoutOperatorFrontier(b *testing.B) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{NodeName: "node-a"})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, request)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(response.Body.Len()), "metrics_bytes/op")
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		handler.Handler().ServeHTTP(recorder, request)
	}
}

func BenchmarkMonitoringMetricsWithOperatorFrontier(b *testing.B) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	frontiers := hatMetrics.NewOperatorFrontierRegistry()
	for i := 0; i < 128; i++ {
		if err := frontiers.Advance(fmt.Sprintf("operator-%03d", i), uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
		NodeName:                 "node-a",
		OperatorFrontier:         frontiers,
		OperatorFrontierObserved: func() uint64 { return 1000 },
	})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.Handler().ServeHTTP(response, request)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(response.Body.Len()), "metrics_bytes/op")
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		handler.Handler().ServeHTTP(recorder, request)
	}
}
