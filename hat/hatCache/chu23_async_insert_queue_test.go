package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestCHU23AsyncInsertQueueRegistryReportsAndFlushes(t *testing.T) {
	trie, journal, buffer := newCHU23AsyncInsertBuffer(t)
	defer trie.Destroy()
	defer journal.Close()
	defer buffer.Close(context.Background())

	registry, err := NewAsyncInsertQueueRegistry(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(" writer ", buffer); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := registry.RegisteredStats("writer"); err != nil {
		t.Fatalf("RegisteredStats() error = %v", err)
	}
	if err := registry.Register("writer", buffer); !errors.Is(err, ErrAsyncInsertQueueExists) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, ErrAsyncInsertQueueExists)
	}

	receipt, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "queue:one", Value: "value"})
	if err != nil {
		t.Fatal(err)
	}
	stats := registry.Stats()
	if len(stats) != 1 || stats[0].Name != "writer" || stats[0].Submitted != 1 || stats[0].Pending != 1 {
		t.Fatalf("Stats() = %#v, want one pending submitted item", stats)
	}
	if err := registry.Flush(context.Background(), "writer"); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	response, err := receipt.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("receipt = %#v/%v, want successful response", response, err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "queue:one"}); !got.OK || got.Value != "value" {
		t.Fatalf("stored value = %#v, want value", got)
	}
	stats = registry.Stats()
	if stats[0].Pending != 0 || stats[0].FlushedItems != 1 || stats[0].FlushedBatches != 1 {
		t.Fatalf("flushed Stats() = %#v, want empty pending and one flushed item/batch", stats)
	}
	if err := registry.Flush(context.Background(), "missing"); !errors.Is(err, ErrAsyncInsertQueueNotFound) {
		t.Fatalf("missing Flush() error = %v, want %v", err, ErrAsyncInsertQueueNotFound)
	}
}

func TestCHU23AsyncInsertQueueRegistryBoundsAndIsolation(t *testing.T) {
	trie, journal, buffer := newCHU23AsyncInsertBuffer(t)
	defer trie.Destroy()
	defer journal.Close()
	defer buffer.Close(context.Background())

	if _, err := NewAsyncInsertQueueRegistry(-1); !errors.Is(err, ErrAsyncInsertQueueRegistryCapacityInvalid) {
		t.Fatalf("negative registry capacity error = %v, want %v", err, ErrAsyncInsertQueueRegistryCapacityInvalid)
	}
	registry, err := NewAsyncInsertQueueRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("first", buffer); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("second", buffer); !errors.Is(err, ErrAsyncInsertQueueRegistryFull) {
		t.Fatalf("full Register() error = %v, want %v", err, ErrAsyncInsertQueueRegistryFull)
	}
	if registry.Unregister("missing") {
		t.Fatal("Unregister(missing) returned true")
	}
	if !registry.Unregister(" first ") {
		t.Fatal("Unregister(first) returned false")
	}
	if len(registry.Stats()) != 0 {
		t.Fatalf("Stats() after unregister = %#v, want empty", registry.Stats())
	}
}

func TestCHU23AsyncInsertQueueMonitoringRequiresAuthAndFlushes(t *testing.T) {
	trie, journal, buffer := newCHU23AsyncInsertBuffer(t)
	defer trie.Destroy()
	defer journal.Close()
	defer buffer.Close(context.Background())
	registry, err := NewAsyncInsertQueueRegistry(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("writer", buffer); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "queue:http", Value: "ok"}); err != nil {
		t.Fatal(err)
	}

	handler := NewMonitoringHandler(trie, MonitoringOptions{
		AuthToken:         "operator-token",
		AsyncInsertQueues: registry,
	})
	mux := handler.Handler()
	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/async-inserts", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized queue status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	statusRequest := httptest.NewRequest(http.MethodGet, "/api/async-inserts", nil)
	statusRequest.Header.Set("Authorization", "Bearer operator-token")
	statusResponse := httptest.NewRecorder()
	mux.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want %d: %s", statusResponse.Code, http.StatusOK, statusResponse.Body.String())
	}
	var status AsyncInsertQueuesResponse
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Queues) != 1 || status.Queues[0].Name != "writer" || status.Queues[0].Pending != 1 {
		t.Fatalf("queue status body = %#v, want one pending queue", status)
	}

	flushRequest := httptest.NewRequest(http.MethodPost, "/api/async-inserts/flush?name=writer", nil)
	flushRequest.Header.Set("Authorization", "Bearer operator-token")
	flushResponse := httptest.NewRecorder()
	mux.ServeHTTP(flushResponse, flushRequest)
	if flushResponse.Code != http.StatusOK {
		t.Fatalf("queue flush = %d, want %d: %s", flushResponse.Code, http.StatusOK, flushResponse.Body.String())
	}
	var flushed AsyncInsertQueueFlushResponse
	if err := json.Unmarshal(flushResponse.Body.Bytes(), &flushed); err != nil {
		t.Fatal(err)
	}
	if !flushed.Flushed || flushed.Name != "writer" || len(flushed.Queues) != 1 || flushed.Queues[0].Pending != 0 {
		t.Fatalf("queue flush body = %#v, want flushed writer with no pending items", flushed)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "queue:http"}); !got.OK || got.Value != "ok" {
		t.Fatalf("HTTP-flushed value = %#v, want ok", got)
	}

	missingRequest := httptest.NewRequest(http.MethodPost, "/api/async-inserts/flush?name=missing", nil)
	missingRequest.Header.Set("Authorization", "Bearer operator-token")
	missingResponse := httptest.NewRecorder()
	mux.ServeHTTP(missingResponse, missingRequest)
	if missingResponse.Code != http.StatusNotFound {
		t.Fatalf("missing queue flush = %d, want %d", missingResponse.Code, http.StatusNotFound)
	}

	openAPI := httptest.NewRecorder()
	mux.ServeHTTP(openAPI, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if openAPI.Code != http.StatusOK || !bytes.Contains(openAPI.Body.Bytes(), []byte("/api/async-inserts")) || !bytes.Contains(openAPI.Body.Bytes(), []byte("/api/async-inserts/flush")) {
		t.Fatalf("OpenAPI status/body = %d/%s, want async queue paths", openAPI.Code, openAPI.Body.String())
	}
}

func TestCHU23AsyncInsertQueueMonitoringIsOffByDefault(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	mux := NewMonitoringHandler(trie, MonitoringOptions{AuthToken: "operator-token"}).Handler()

	request := httptest.NewRequest(http.MethodGet, "/api/async-inserts", nil)
	request.Header.Set("Authorization", "Bearer operator-token")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("default queue status = %d, want %d", response.Code, http.StatusNotFound)
	}

	openAPI := httptest.NewRecorder()
	mux.ServeHTTP(openAPI, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if openAPI.Code != http.StatusOK || bytes.Contains(openAPI.Body.Bytes(), []byte("/api/async-inserts")) {
		t.Fatalf("default OpenAPI status/body = %d/%s, want no async queue paths", openAPI.Code, openAPI.Body.String())
	}
}

func newCHU23AsyncInsertBuffer(t *testing.T) (*HatTrie, *CommandJournal, *AsyncInsertBuffer) {
	t.Helper()
	trie := CreateHatTrie()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 16,
	})
	if err != nil {
		trie.Destroy()
		t.Fatal(err)
	}
	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     4,
		Capacity:      8,
		FlushInterval: time.Hour,
	})
	if err != nil {
		journal.Close()
		trie.Destroy()
		t.Fatal(err)
	}
	return trie, journal, buffer
}
