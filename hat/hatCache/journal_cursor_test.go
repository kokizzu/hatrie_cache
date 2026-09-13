package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
)

func TestMonitoringHandlerJournalCursorResumesAndBinds(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "cursor:" + string(rune('a'+index)),
			Value:   "value",
		})
		if !response.OK {
			t.Fatalf("journaled SETSTR response = %#v, want ok", response)
		}
	}

	const secret = "0123456789abcdef"
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:             journal,
		JournalCursorSecret: secret,
	}).Handler()

	firstResponse := httptest.NewRecorder()
	handler.ServeHTTP(firstResponse, httptest.NewRequest(http.MethodGet, "/api/journal?limit=1", nil))
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first journal status = %d, want 200", firstResponse.Code)
	}
	var first CommandJournalTail
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		t.Fatalf("first journal JSON error = %v", err)
	}
	if len(first.Entries) != 1 || first.Entries[0].Sequence != 1 || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first journal tail = %#v, want sequence 1 with next cursor", first)
	}

	secondQuery := url.Values{"cursor": {first.NextCursor}, "limit": {"1"}}
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+secondQuery.Encode(), nil))
	if secondResponse.Code != http.StatusOK {
		t.Fatalf("second journal status = %d, want 200", secondResponse.Code)
	}
	var second CommandJournalTail
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &second); err != nil {
		t.Fatalf("second journal JSON error = %v", err)
	}
	if len(second.Entries) != 1 || second.Entries[0].Sequence != 2 || second.NextCursor == "" {
		t.Fatalf("second journal tail = %#v, want sequence 2 with next cursor", second)
	}

	tampered := "A" + first.NextCursor[1:]
	if tampered == first.NextCursor {
		tampered = "B" + first.NextCursor[1:]
	}
	tamperedQuery := url.Values{"cursor": {tampered}}
	tamperedResponse := httptest.NewRecorder()
	handler.ServeHTTP(tamperedResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+tamperedQuery.Encode(), nil))
	if tamperedResponse.Code != http.StatusBadRequest {
		t.Fatalf("tampered cursor status = %d, want 400", tamperedResponse.Code)
	}

	wrongSecretHandler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:             journal,
		JournalCursorSecret: "fedcba9876543210",
	}).Handler()
	wrongSecretResponse := httptest.NewRecorder()
	wrongSecretHandler.ServeHTTP(wrongSecretResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+secondQuery.Encode(), nil))
	if wrongSecretResponse.Code != http.StatusBadRequest {
		t.Fatalf("wrong-secret cursor status = %d, want 400", wrongSecretResponse.Code)
	}

	ambiguousQuery := url.Values{"after_sequence": {"1"}, "cursor": {first.NextCursor}}
	ambiguousResponse := httptest.NewRecorder()
	handler.ServeHTTP(ambiguousResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+ambiguousQuery.Encode(), nil))
	if ambiguousResponse.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous cursor status = %d, want 400", ambiguousResponse.Code)
	}

	binaryRequest := httptest.NewRequest(http.MethodGet, "/api/journal?limit=1", nil)
	binaryRequest.Header.Set("Accept", commandJournalTailBinaryContentType)
	binaryResponse := httptest.NewRecorder()
	handler.ServeHTTP(binaryResponse, binaryRequest)
	if binaryResponse.Code != http.StatusOK {
		t.Fatalf("binary journal status = %d, want 200", binaryResponse.Code)
	}
	if got := binaryResponse.Header().Get(commandJournalNextCursorHeader); got == "" {
		t.Fatal("binary journal response did not return a next cursor header")
	}
	if got := binaryResponse.Header().Get(commandJournalNextCursorHeader); got != first.NextCursor {
		t.Fatalf("binary next cursor = %q, want JSON cursor %q", got, first.NextCursor)
	}
	if _, err := decodeCommandJournalTailBinaryResponse(binaryResponse.Body); err != nil {
		t.Fatalf("binary journal decode error = %v", err)
	}

	otherJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "other.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal(other) error = %v", err)
	}
	defer otherJournal.Close()
	otherHandler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:             otherJournal,
		JournalCursorSecret: secret,
	}).Handler()
	otherResponse := httptest.NewRecorder()
	otherHandler.ServeHTTP(otherResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+secondQuery.Encode(), nil))
	if otherResponse.Code != http.StatusBadRequest {
		t.Fatalf("cross-journal cursor status = %d, want 400", otherResponse.Code)
	}

	defaultHandler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler()
	defaultQuery := url.Values{"cursor": {first.NextCursor}}
	defaultResponse := httptest.NewRecorder()
	defaultHandler.ServeHTTP(defaultResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+defaultQuery.Encode(), nil))
	if defaultResponse.Code != http.StatusBadRequest {
		t.Fatalf("unconfigured cursor status = %d, want 400", defaultResponse.Code)
	}

	invalidHandler := NewMonitoringHandler(trie, MonitoringOptions{
		Journal:             journal,
		JournalCursorSecret: "too-short",
	}).Handler()
	invalidResponse := httptest.NewRecorder()
	invalidHandler.ServeHTTP(invalidResponse, httptest.NewRequest(http.MethodGet, "/api/journal?"+defaultQuery.Encode(), nil))
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("invalid-secret cursor status = %d, want 400", invalidResponse.Code)
	}
}
