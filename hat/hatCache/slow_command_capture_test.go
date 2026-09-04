package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMonitoringSlowCommandCaptureIsOptInAndRedactsValues(t *testing.T) {
	trie := newTestTrie(t)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		SlowCommandThreshold: time.Nanosecond,
		SlowCommandCapacity:  2,
	}).Handler()

	commandResponse := httptest.NewRecorder()
	commandRequest := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"slow:key","value":"super-secret"}`))
	commandRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(commandResponse, commandRequest)
	if commandResponse.Code != http.StatusOK {
		t.Fatalf("command status = %d, want %d: %s", commandResponse.Code, http.StatusOK, commandResponse.Body.String())
	}

	slowResponse := httptest.NewRecorder()
	handler.ServeHTTP(slowResponse, httptest.NewRequest(http.MethodGet, "/api/commands/slow", nil))
	if slowResponse.Code != http.StatusOK {
		t.Fatalf("slow-command status = %d, want %d: %s", slowResponse.Code, http.StatusOK, slowResponse.Body.String())
	}
	var report SlowCommandReport
	if err := json.Unmarshal(slowResponse.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode slow-command report: %v", err)
	}
	if !report.Enabled || report.ThresholdNS != 1 || report.Capacity != 2 || len(report.Entries) != 1 {
		t.Fatalf("slow-command report = %#v, want one enabled entry", report)
	}
	entry := report.Entries[0]
	if entry.Command != "SETSTR" || entry.Key != "slow:key" || entry.Status != http.StatusOK || !entry.OK || entry.DurationNS < 1 {
		t.Fatalf("slow-command entry = %#v", entry)
	}
	if strings.Contains(slowResponse.Body.String(), "super-secret") {
		t.Fatal("slow-command report leaked command value")
	}
}

func TestMonitoringSlowCommandCaptureDisabledByDefault(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/commands/slow", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("disabled slow-command status = %d, want %d", response.Code, http.StatusOK)
	}
	var report SlowCommandReport
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode disabled slow-command report: %v", err)
	}
	if report.Enabled || report.ThresholdNS != 0 || report.Capacity != 0 || len(report.Entries) != 0 {
		t.Fatalf("disabled slow-command report = %#v, want empty disabled report", report)
	}
}

func TestMonitoringSlowCommandCaptureKeepsNewestEntries(t *testing.T) {
	capture := newMonitoringSlowCommandCapture(0, 2)
	for _, key := range []string{"one", "two", "three"} {
		capture.add(time.Now(), CacheCommandRequest{Command: "GET", Key: key}, CacheCommandResponse{OK: true}, http.StatusOK)
	}
	report := capture.report()
	if len(report.Entries) != 2 {
		t.Fatalf("captured entries = %d, want 2", len(report.Entries))
	}
	if got := report.Entries[0].Key; got != "three" {
		t.Fatalf("newest captured key = %q, want three", got)
	}
	if got := report.Entries[1].Key; got != "two" {
		t.Fatalf("second captured key = %q, want two", got)
	}
	if normalizeMonitoringSlowCommandCaptureCapacity(0) != DefaultMonitoringSlowCommandCaptureCapacity {
		t.Fatalf("default slow-command capacity = %d, want %d", normalizeMonitoringSlowCommandCaptureCapacity(0), DefaultMonitoringSlowCommandCaptureCapacity)
	}
	if normalizeMonitoringSlowCommandCaptureCapacity(MaxMonitoringSlowCommandCaptureCapacity+1) != MaxMonitoringSlowCommandCaptureCapacity {
		t.Fatalf("maximum slow-command capacity was not enforced")
	}
}

func TestMonitoringSlowCommandCaptureEndpointRequiresOperatorAuth(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		AuthToken:            "operator-secret",
		SlowCommandThreshold: time.Nanosecond,
	}).Handler()
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/commands/slow", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized slow-command status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}
	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/commands/slow", nil)
	request.Header.Set("X-Hatrie-Auth-Token", "operator-secret")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized slow-command status = %d, want %d", authorized.Code, http.StatusOK)
	}
}
