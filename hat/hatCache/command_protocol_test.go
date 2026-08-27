package hatCache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCommand"
)

func TestMonitoringCommandProtocolNegotiation(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"versioned","value":"ok"}`))
	request.Header.Set(hatCommand.HeaderProtocolVersion, "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("versioned command status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get(hatCommand.HeaderProtocolVersion); got != "1" {
		t.Fatalf("response protocol version = %q, want 1", got)
	}
	if got := response.Header().Get(hatCommand.HeaderProtocolSupportedVersions); got != "1" {
		t.Fatalf("response supported protocols = %q, want 1", got)
	}
	if value := ht.GetString("versioned"); value != "ok" {
		t.Fatalf("versioned command stored %q, want ok", value)
	}
}

func TestMonitoringCommandProtocolRejectsIncompatibleBeforeMutation(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"blocked","value":"no"}`))
	request.Header.Set(hatCommand.HeaderProtocolVersion, "2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("incompatible command status = %d, want 426: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get(hatCommand.HeaderProtocolSupportedVersions); got != "1" {
		t.Fatalf("response supported protocols = %q, want 1", got)
	}
	if ht.Exists("blocked") {
		t.Fatal("incompatible protocol command mutated the cache")
	}
}
