package hatCache

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkCHU34HTTPMutationRequest(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	handler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler()
	body := "{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:benchmark', 'value')\",\"mutation_id\":\"http-benchmark-1\"}"
	warmup := httptest.NewRecorder()
	handler.ServeHTTP(warmup, httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body)))
	if warmup.Code != http.StatusOK {
		b.Fatalf("warm-up SQL mutation status = %d, body = %s", warmup.Code, warmup.Body.String())
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
	}
}
