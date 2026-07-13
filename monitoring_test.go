package hatriecache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMonitoringHandlerExposesHealthStatsAndEntries(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("session:1", "active user")
	ht.UpsertCounter("counter:views", 42)
	if !ht.Expire("session:1", time.Minute) {
		t.Fatal("Expire(session:1) = false, want true")
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName: "test-node",
		StartAt:  now.Add(-time.Hour),
	}).Handler()

	healthResp := httptest.NewRecorder()
	handler.ServeHTTP(healthResp, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthResp.Code)
	}
	var health MonitoringHealth
	if err := json.Unmarshal(healthResp.Body.Bytes(), &health); err != nil {
		t.Fatalf("health JSON error = %v", err)
	}
	if health.Node != "test-node" || health.Status != "online" {
		t.Fatalf("health = %#v, want test-node online", health)
	}

	statsResp := httptest.NewRecorder()
	handler.ServeHTTP(statsResp, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
	if statsResp.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", statsResp.Code)
	}
	var stats CacheStats
	if err := json.Unmarshal(statsResp.Body.Bytes(), &stats); err != nil {
		t.Fatalf("stats JSON error = %v", err)
	}
	if stats.Writes == 0 {
		t.Fatalf("stats writes = 0, want existing cache writes")
	}

	entriesResp := httptest.NewRecorder()
	handler.ServeHTTP(entriesResp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil))
	if entriesResp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", entriesResp.Code)
	}
	var entries MonitoringEntriesResponse
	if err := json.Unmarshal(entriesResp.Body.Bytes(), &entries); err != nil {
		t.Fatalf("entries JSON error = %v", err)
	}
	if len(entries.Entries) != 1 {
		t.Fatalf("entries len = %d, want 1: %#v", len(entries.Entries), entries.Entries)
	}
	entry := entries.Entries[0]
	if entry.Key != "session:1" || entry.Type != "string" || entry.ValuePreview != "active user" {
		t.Fatalf("entry = %#v, want session string preview", entry)
	}
	if entry.TTLMillis == nil || *entry.TTLMillis != int64(time.Minute/time.Millisecond) {
		t.Fatalf("entry TTL = %v, want 60000", entry.TTLMillis)
	}
}

func TestMonitoringHandlerServesStaticWebDir(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.html")
	if err := os.WriteFile(indexPath, []byte("monitoring ui"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{WebDir: dir}).Handler()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("static status = %d, want 200", resp.Code)
	}
	if got := resp.Body.String(); got != "monitoring ui" {
		t.Fatalf("static body = %q, want monitoring ui", got)
	}
}

func TestMonitoringHandlerExecutesCommands(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	setResp := httptest.NewRecorder()
	handler.ServeHTTP(setResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"name","value":"ivi"}`)))
	if setResp.Code != http.StatusOK {
		t.Fatalf("SETSTR status = %d, want 200", setResp.Code)
	}
	var setResult CacheCommandResponse
	if err := json.Unmarshal(setResp.Body.Bytes(), &setResult); err != nil {
		t.Fatalf("SETSTR JSON error = %v", err)
	}
	if !setResult.OK {
		t.Fatalf("SETSTR response = %#v, want ok", setResult)
	}

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"name"}`)))
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.Code)
	}
	var getResult CacheCommandResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &getResult); err != nil {
		t.Fatalf("GET JSON error = %v", err)
	}
	if !getResult.OK || getResult.Value != "ivi" {
		t.Fatalf("GET response = %#v, want ivi", getResult)
	}
}

func TestMonitoringHandlerRejectsInvalidCommandRequests(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, httptest.NewRequest(method, "/api/commands", nil))
		if resp.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405", method, resp.Code)
		}
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"x"} trailing`)))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want 400", resp.Code)
	}
}

func TestMonitoringHandlerForcesSnapshotWhenConfigured(t *testing.T) {
	ht := newTestTrie(t)
	called := false
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Snapshot: func() error {
			called = true
			return nil
		},
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", resp.Code)
	}
	if !called {
		t.Fatal("snapshot callback was not called")
	}
	var result CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("snapshot JSON error = %v", err)
	}
	if !result.OK {
		t.Fatalf("snapshot response = %#v, want ok", result)
	}
}

func TestMonitoringHandlerRejectsForcedSnapshotWhenUnconfigured(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusConflict {
		t.Fatalf("snapshot status = %d, want 409", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/snapshot", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("snapshot GET status = %d, want 405", resp.Code)
	}
}
