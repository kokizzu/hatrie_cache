package hatCache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func BenchmarkMonitoringCommandHTTP(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
		IdempotencyCapacity: 1 << 16,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	handler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		body := []byte(`{"command":"SET","key":"async-bench:` + strconv.Itoa(index) + `","value":"value","idempotency_key":"bench-` + strconv.Itoa(index) + `"}`)
		request := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			b.Fatalf("synchronous command status = %d, want 200", response.Code)
		}
	}
}

func BenchmarkMonitoringAsyncCommandHTTPAdmission(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
		IdempotencyCapacity: 1 << 16,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	monitoringHandler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:                    journal,
		AsyncCommands:              true,
		AsyncCommandStatusCapacity: MaxMonitoringAsyncCommandStatusCapacity,
	})
	handler := monitoringHandler.Handler()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		key := "bench-" + strconv.Itoa(index)
		body := []byte(`{"command":"SET","key":"async-bench:` + strconv.Itoa(index) + `","value":"value","idempotency_key":"` + key + `"}`)
		request := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		request.Header.Set("X-Hatrie-Async", "true")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			b.Fatalf("async command status = %d, want 202", response.Code)
		}
		b.StopTimer()
		waitForAsyncCommandBenchmark(monitoringHandler, key, b)
		b.StartTimer()
	}
}

func waitForAsyncCommandBenchmark(handler *MonitoringHandler, key string, b *testing.B) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		handler.asyncCommandsMu.Lock()
		entry, ok := handler.asyncCommands[key]
		completed := ok && entry.completed
		handler.asyncCommandsMu.Unlock()
		if completed {
			return
		}
		time.Sleep(time.Microsecond)
	}
	b.Fatalf("async command %q did not complete", key)
}
