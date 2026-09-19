package hatCache

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestCHU34SQLMutationHTTPIdempotencyReturnsDurableRetryResult(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	handler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler()

	body := "{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:retry', 'first')\",\"mutation_id\":\"http-mutation-1\"}"
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body)))
	if first.Code != http.StatusOK {
		t.Fatalf("first SQL mutation status = %d, body = %s", first.Code, first.Body.String())
	}
	if !strings.Contains(first.Body.String(), "\"affected\":1") {
		t.Fatalf("first SQL mutation body = %s, want affected=1", first.Body.String())
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body)))
	if second.Code != http.StatusOK || second.Body.String() != first.Body.String() {
		t.Fatalf("retry SQL mutation = status %d body %s, want the original status/body %d %s", second.Code, second.Body.String(), first.Code, first.Body.String())
	}
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "http:retry"}); response.Value != "first" {
		t.Fatalf("retry SQL mutation changed stored value: %#v", response)
	}

	conflict := httptest.NewRecorder()
	conflictBody := "{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:retry', 'different')\",\"mutation_id\":\"http-mutation-1\"}"
	handler.ServeHTTP(conflict, httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(conflictBody)))
	if conflict.Code != http.StatusBadRequest {
		t.Fatalf("conflicting SQL mutation status = %d, body = %s, want 400", conflict.Code, conflict.Body.String())
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}

	reopened, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("reopen CommandJournal error = %v", err)
	}
	defer reopened.Close()
	replayed := newTestTrie(t)
	if _, err := reopened.Replay(replayed, 0); err != nil {
		t.Fatalf("reopened.Replay() error = %v", err)
	}
	replayedHandler := NewMonitoringHandler(replayed, MonitoringOptions{Journal: reopened}).Handler()
	afterRestart := httptest.NewRecorder()
	replayedHandler.ServeHTTP(afterRestart, httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body)))
	if afterRestart.Code != first.Code || afterRestart.Body.String() != first.Body.String() {
		t.Fatalf("post-restart SQL mutation = status %d body %s, want the original status/body %d %s", afterRestart.Code, afterRestart.Body.String(), first.Code, first.Body.String())
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "http:retry"}); response.Value != "first" {
		t.Fatalf("post-restart retry changed stored value: %#v", response)
	}
}

func TestCHU34SQLMutationHTTPRequiresJournalAndMutationID(t *testing.T) {
	trie := newTestTrie(t)
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader("{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:no-journal', 'value')\",\"mutation_id\":\"missing-journal\"}"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing journal status = %d, body = %s, want 400", response.Code, response.Body.String())
	}

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		IdempotencyCapacity: 16,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	handler = NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader("{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:no-id', 'value')\"}"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing mutation id status = %d, body = %s, want the existing read-only path rejection", response.Code, response.Body.String())
	}
	if value := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "http:no-id"}); value.Value != "" {
		t.Fatalf("missing mutation id unexpectedly wrote a value: %#v", value)
	}

	maintenanceHandler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:             journal,
		MaintenanceReadOnly: true,
	}).Handler()
	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader("{\"query\":\"INSERT INTO cache (key, value) VALUES ('http:maintenance', 'value')\",\"mutation_id\":\"maintenance-1\"}"))
	response = httptest.NewRecorder()
	maintenanceHandler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("maintenance mutation status = %d, body = %s, want 503", response.Code, response.Body.String())
	}
	if value := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "http:maintenance"}); value.Value != "" {
		t.Fatalf("maintenance mutation unexpectedly wrote a value: %#v", value)
	}
}
