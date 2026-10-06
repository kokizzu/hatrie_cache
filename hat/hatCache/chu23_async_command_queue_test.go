package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCHU23CommandJournalAsyncQueueStatsAndFlush(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var syncOnce sync.Once
	journal.mu.Lock()
	journal.syncHook = func() error {
		syncOnce.Do(func() { close(syncStarted) })
		<-releaseSync
		return journal.file.Sync()
	}
	journal.mu.Unlock()

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command: "SET",
		Key:     "chu23:secret-key",
		Value:   "chu23:secret-value",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("async submission did not reach journal sync")
	}

	stats := journal.AsyncCommandQueueStats()
	if stats.Capacity != 4 || stats.Pending != 1 || stats.Accepted != 1 || stats.Completed != 0 {
		t.Fatalf("queue stats before flush = %#v, want one pending admitted command", stats)
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- journal.FlushAsyncCommands(context.Background()) }()
	select {
	case err := <-flushDone:
		t.Fatalf("flush completed before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseSync)
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats = journal.AsyncCommandQueueStats()
	if stats.Pending != 0 || stats.QueueDepth != 0 || stats.Completed != 1 || stats.Rejected != 0 || stats.Failed != 0 {
		t.Fatalf("queue stats after flush = %#v, want no pending successful command", stats)
	}
}

func TestCHU23CommandJournalAsyncQueueFlushCancellationDoesNotCancelWrites(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 2,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var syncOnce sync.Once
	journal.mu.Lock()
	journal.syncHook = func() error {
		syncOnce.Do(func() { close(syncStarted) })
		<-releaseSync
		return journal.file.Sync()
	}
	journal.mu.Unlock()

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "chu23:cancel", Value: "value"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("async submission did not reach journal sync")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := journal.FlushAsyncCommands(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled flush error = %v, want context canceled", err)
	}
	close(releaseSync)
	response, err := submission.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("submission after cancelled flush = %#v/%v, want success", response, err)
	}
	if err := journal.FlushAsyncCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCHU23AsyncCommandQueueHTTPIsAuthenticatedAndDefaultOff(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	server := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{
		AuthToken:     "operator-token",
		Journal:       journal,
		AsyncCommands: true,
	}).Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/commands/async", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated queue status = %d, want 401", response.StatusCode)
	}

	request, err = http.NewRequest(http.MethodGet, server.URL+"/api/commands/async", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var status AsyncCommandQueueResponse
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || status.Queue.Capacity != 4 || status.Queue.Pending != 0 {
		t.Fatalf("queue status = %d/%#v, want empty configured queue", response.StatusCode, status)
	}

	body := []byte(`{"command":"SET","key":"chu23:http-secret-key","value":"chu23:http-secret-value","idempotency_key":"chu23:http-1"}`)
	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	asyncResponseBody, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("async command status = %d, body = %s, want 202", response.StatusCode, asyncResponseBody)
	}

	request, err = http.NewRequest(http.MethodGet, server.URL+"/api/commands/async", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || bytes.Contains(data, []byte("chu23:http-secret-key")) || bytes.Contains(data, []byte("chu23:http-secret-value")) {
		t.Fatalf("queue response = %d/%s, must not expose command data", response.StatusCode, data)
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/commands/async/flush", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-token")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var flushed AsyncCommandQueueFlushResponse
	if err := json.NewDecoder(response.Body).Decode(&flushed); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || flushed.Queue.Pending != 0 {
		t.Fatalf("queue flush = %d/%#v, want empty queue", response.StatusCode, flushed)
	}

	defaultServer := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler())
	defer defaultServer.Close()
	request, err = http.NewRequest(http.MethodGet, defaultServer.URL+"/api/commands/async", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("default-off queue status = %d, want 404", response.StatusCode)
	}

	openAPIRequest, err := http.NewRequest(http.MethodGet, server.URL+"/openapi.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	openAPIRequest.Header.Set("Authorization", "Bearer operator-token")
	openAPIResponse, err := http.DefaultClient.Do(openAPIRequest)
	if err != nil {
		t.Fatal(err)
	}
	openAPIData, err := io.ReadAll(openAPIResponse.Body)
	openAPIResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if openAPIResponse.StatusCode != http.StatusOK || !strings.Contains(string(openAPIData), "/api/commands/async") || !strings.Contains(string(openAPIData), "/api/commands/async/flush") {
		t.Fatalf("OpenAPI queue routes = %d/%s, want both queue routes", openAPIResponse.StatusCode, openAPIData)
	}
}
