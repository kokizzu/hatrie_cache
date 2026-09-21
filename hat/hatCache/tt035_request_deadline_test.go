package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTT035MonitoringRequestTimeoutDisabledByDefault(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{})
	parent := context.Background()
	got, cancel, err := handler.commandContext(parent)
	if err != nil {
		t.Fatalf("commandContext(default) error = %v", err)
	}
	defer cancel()
	if got != parent {
		t.Fatal("default monitoring command context was wrapped")
	}
}

func TestTT035MonitoringRequestTimeoutConfigured(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{RequestTimeout: time.Second})
	got, cancel, err := handler.commandContext(context.Background())
	if err != nil {
		t.Fatalf("commandContext(configured) error = %v", err)
	}
	defer cancel()
	if _, ok := got.Deadline(); !ok {
		t.Fatal("configured monitoring command context has no deadline")
	}
}

func TestTT035GRPCRequestTimeoutConfigured(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{RequestTimeout: time.Second})
	got, cancel, err := server.commandContext(context.Background())
	if err != nil {
		t.Fatalf("commandContext(configured) error = %v", err)
	}
	defer cancel()
	if _, ok := got.Deadline(); !ok {
		t.Fatal("configured gRPC command context has no deadline")
	}
}

func TestTT035GRPCStreamCommandTimeoutRefreshesPerMessage(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{RequestTimeout: time.Second})
	first, firstCancel, err := server.commandContext(context.Background())
	if err != nil {
		t.Fatalf("first commandContext() error = %v", err)
	}
	defer firstCancel()
	second, secondCancel, err := server.commandContext(context.Background())
	if err != nil {
		t.Fatalf("second commandContext() error = %v", err)
	}
	defer secondCancel()
	firstDeadline, firstOK := first.Deadline()
	secondDeadline, secondOK := second.Deadline()
	if !firstOK || !secondOK {
		t.Fatal("stream command contexts must both have deadlines")
	}
	if !secondDeadline.After(firstDeadline) && secondDeadline != firstDeadline {
		t.Fatalf("second stream deadline = %v, first = %v", secondDeadline, firstDeadline)
	}
}

func TestTT035MonitoringCommandRouteEnforcesConfiguredTimeout(t *testing.T) {
	trie := newTestTrie(t)
	handler := NewMonitoringHandler(trie, MonitoringOptions{RequestTimeout: time.Nanosecond}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"deadline","value":"must-not-write"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("command status = %d, want %d; body = %s", response.Code, http.StatusRequestTimeout, response.Body.String())
	}
	if trie.Exists("deadline") {
		t.Fatal("timed-out command mutated the trie")
	}
}
