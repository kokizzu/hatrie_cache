package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandJournalSubmitAsyncCommandCompletionCallback(t *testing.T) {
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

	completed := make(chan CacheCommandResponse, 1)
	submission, err := journal.submitAsyncCommand(trie, CacheCommandRequest{
		Command:        "SET",
		Key:            "async:callback",
		Value:          "value",
		IdempotencyKey: "callback-1",
	}, func(response CacheCommandResponse) {
		completed <- response
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-completed:
		if !response.OK {
			t.Fatalf("completion callback response = %#v, want success", response)
		}
	default:
		t.Fatal("completion callback was not invoked")
	}
}

func TestCommandJournalSubmitAsyncCommandHTTP(t *testing.T) {
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

	handler := NewMonitoringHandler(trie, MonitoringOptions{
		AuthToken:     "operator-token",
		Journal:       journal,
		AsyncCommands: true,
	})
	server := httptest.NewServer(handler.Handler())
	defer server.Close()

	body := []byte(`{"command":"SET","key":"async:http","value":"value","idempotency_key":"http-request-1"}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer operator-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		Accepted       bool   `json:"accepted"`
		Status         string `json:"status"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("async status = %d, want 202", response.StatusCode)
	}
	if !accepted.Accepted || accepted.Status != "pending" || accepted.IdempotencyKey != "http-request-1" {
		t.Fatalf("async response = %#v, want accepted pending response", accepted)
	}

	var status struct {
		IdempotencyKey string                `json:"idempotency_key"`
		Status         string                `json:"status"`
		Response       *CacheCommandResponse `json:"response,omitempty"`
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		statusRequest, err := http.NewRequest(http.MethodGet, server.URL+"/api/commands/status?idempotency_key=http-request-1", nil)
		if err != nil {
			t.Fatal(err)
		}
		statusRequest.Header.Set("Authorization", "Bearer operator-token")
		statusResponse, err := http.DefaultClient.Do(statusRequest)
		if err != nil {
			t.Fatal(err)
		}
		statusErr := json.NewDecoder(statusResponse.Body).Decode(&status)
		statusResponse.Body.Close()
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.Status == "completed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if status.IdempotencyKey != "http-request-1" || status.Status != "completed" || status.Response == nil || !status.Response.OK {
		t.Fatalf("async completion status = %#v, want completed success", status)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:http"}); got.Value != "value" {
		t.Fatalf("async HTTP value = %#v, want value", got)
	}

	defaultHandler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal})
	defaultServer := httptest.NewServer(defaultHandler.Handler())
	defer defaultServer.Close()
	request, err = http.NewRequest(http.MethodPost, defaultServer.URL+"/api/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("async request with default-off handler status = %d, want 409", response.StatusCode)
	}
}

func TestMonitoringAsyncCommandSupportsPreferAndRejectsConflict(t *testing.T) {
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
		Journal:       journal,
		AsyncCommands: true,
	}).Handler())
	defer server.Close()

	body := []byte(`{"command":"SET","key":"async:prefer","value":"one","idempotency_key":"prefer-1"}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Prefer", "respond-async")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var accepted AsyncCommandAcceptedResponse
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || !accepted.Accepted || accepted.Status != "pending" {
		t.Fatalf("Prefer async response = status %d, %#v, want 202 pending", response.StatusCode, accepted)
	}
	waitForAsyncCommandCompletion(t, server.URL, "prefer-1", "")

	duplicateRequest, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	duplicateRequest.Header.Set("Content-Type", "application/json")
	duplicateRequest.Header.Set("Accept", "application/json")
	duplicateRequest.Header.Set("X-Hatrie-Async", "true")
	duplicateResponse, err := http.DefaultClient.Do(duplicateRequest)
	if err != nil {
		t.Fatal(err)
	}
	var duplicate AsyncCommandAcceptedResponse
	if err := json.NewDecoder(duplicateResponse.Body).Decode(&duplicate); err != nil {
		duplicateResponse.Body.Close()
		t.Fatal(err)
	}
	duplicateResponse.Body.Close()
	if duplicateResponse.StatusCode != http.StatusOK || !duplicate.Accepted || duplicate.Status != "completed" || duplicate.Response == nil || !duplicate.Response.OK {
		t.Fatalf("duplicate async response = status %d, %#v, want 200 completed success", duplicateResponse.StatusCode, duplicate)
	}

	conflictBody := []byte(`{"command":"SET","key":"async:prefer","value":"two","idempotency_key":"prefer-1"}`)
	conflictRequest, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader(conflictBody))
	if err != nil {
		t.Fatal(err)
	}
	conflictRequest.Header.Set("Content-Type", "application/json")
	conflictRequest.Header.Set("Accept", "application/json")
	conflictRequest.Header.Set("X-Hatrie-Async", "true")
	conflictResponse, err := http.DefaultClient.Do(conflictRequest)
	if err != nil {
		t.Fatal(err)
	}
	conflictResponse.Body.Close()
	if conflictResponse.StatusCode != http.StatusConflict {
		t.Fatalf("idempotency conflict status = %d, want 409", conflictResponse.StatusCode)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:prefer"}); got.Value != "one" {
		t.Fatalf("idempotency conflict changed value to %#v, want one", got.Value)
	}
}

func TestMonitoringAsyncCommandRequiresAuthAndIdempotency(t *testing.T) {
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

	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader([]byte(`{"command":"SET","key":"async:auth","value":"value","idempotency_key":"auth-1"}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated async command status = %d, want 401", response.StatusCode)
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader([]byte(`{"command":"SET","key":"async:auth","value":"value"}`)))
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
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("missing idempotency key status = %d, want 409", response.StatusCode)
	}

	statusRequest, err := http.NewRequest(http.MethodGet, server.URL+"/api/commands/status?idempotency_key=auth-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	statusResponse, err := http.DefaultClient.Do(statusRequest)
	if err != nil {
		t.Fatal(err)
	}
	statusResponse.Body.Close()
	if statusResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated async status = %d, want 401", statusResponse.StatusCode)
	}
}

func TestMonitoringAsyncCommandStatusSurvivesHandlerRecreation(t *testing.T) {
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

	firstServer := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{
		Journal:       journal,
		AsyncCommands: true,
	}).Handler())

	request, err := http.NewRequest(http.MethodPost, firstServer.URL+"/api/commands", bytes.NewReader([]byte(`{"command":"SET","key":"async:restart","value":"value","idempotency_key":"restart-1"}`)))
	if err != nil {
		firstServer.Close()
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		firstServer.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		firstServer.Close()
		t.Fatalf("initial async status = %d, want 202", response.StatusCode)
	}
	waitForAsyncCommandCompletion(t, firstServer.URL, "restart-1", "")
	firstServer.Close()

	secondServer := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{
		Journal:       journal,
		AsyncCommands: true,
	}).Handler())
	defer secondServer.Close()
	status := waitForAsyncCommandCompletion(t, secondServer.URL, "restart-1", "")
	if status.Response == nil || !status.Response.OK {
		t.Fatalf("recreated handler status = %#v, want durable completed response", status)
	}
}

func TestMonitoringAsyncCommandRejectsLeaderEnforcement(t *testing.T) {
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
		EnforceLeaderWrites: true,
		Journal:             journal,
		AsyncCommands:       true,
	}).Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", bytes.NewReader([]byte(`{"command":"SET","key":"async:leader","value":"value","idempotency_key":"leader-1"}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Hatrie-Async", "true")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("leader-enforced async status = %d, want 409", response.StatusCode)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:leader"}); got.Value == "value" {
		t.Fatal("leader-enforced async command mutated the trie")
	}
}

func waitForAsyncCommandCompletion(t *testing.T, serverURL, key, authToken string) AsyncCommandStatusResponse {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var status AsyncCommandStatusResponse
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, serverURL+"/api/commands/status?idempotency_key="+key, nil)
		if err != nil {
			t.Fatal(err)
		}
		if authToken != "" {
			request.Header.Set("Authorization", "Bearer "+authToken)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&status)
		response.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("async status endpoint status = %d, want 200", response.StatusCode)
		}
		if status.Status == "completed" {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("async command %q did not complete: %#v", key, status)
	return AsyncCommandStatusResponse{}
}
