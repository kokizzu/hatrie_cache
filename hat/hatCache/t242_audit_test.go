package hatCache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestT242AuditAllOperationsRecordsUninstrumentedRequest(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	handler := NewMonitoringHandler(nil, MonitoringOptions{
		NodeName:           "node-a",
		AuditLog:           logger,
		AuditAllOperations: true,
	}).Handler()

	request := httptest.NewRequest(http.MethodGet, "/api/config?secret=query-value", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	events := decodeT242AuditEvents(t, output.Bytes())
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1: %s", len(events), output.String())
	}
	event := events[0]
	if event.Action != "http.request" || event.Method != http.MethodGet || event.Path != "/api/config" || event.Status != http.StatusOK || !event.OK {
		t.Fatalf("unexpected event: %#v", event)
	}
	if strings.Contains(output.String(), "query-value") {
		t.Fatalf("audit output leaked query value: %s", output.String())
	}
}

func TestT242AuditAllOperationsDoesNotDuplicateDetailedEvent(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	handler := NewMonitoringHandler(nil, MonitoringOptions{
		NodeName:           "node-a",
		AuditLog:           logger,
		AuditAllOperations: true,
	})
	next := handler.auditAllOperations(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.auditHTTP(r, AuditEvent{Action: "detailed.operation", OK: true, Status: http.StatusNoContent})
		w.WriteHeader(http.StatusNoContent)
	}))
	next.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/data", nil))
	events := decodeT242AuditEvents(t, output.Bytes())
	if len(events) != 1 || events[0].Action != "detailed.operation" {
		t.Fatalf("audit events = %#v, want one detailed event", events)
	}
}

func TestT242AuditAllOperationsIsOptIn(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	handler := NewMonitoringHandler(nil, MonitoringOptions{AuditLog: logger}).Handler()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if output.Len() != 0 {
		t.Fatalf("default audit output = %q, want empty", output.String())
	}
}

func TestT242AuditAllOperationsAuditsUnauthorizedWithoutSecrets(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	handler := NewMonitoringHandler(nil, MonitoringOptions{
		AuthToken:          "operator-secret",
		AuditLog:           logger,
		AuditAllOperations: true,
	}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/health?secret=query-value", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	events := decodeT242AuditEvents(t, output.Bytes())
	if len(events) != 1 || events[0].Status != http.StatusUnauthorized || events[0].OK {
		t.Fatalf("unexpected unauthorized events: %#v", events)
	}
	if strings.Contains(output.String(), "operator-secret") || strings.Contains(output.String(), "query-value") {
		t.Fatalf("audit output leaked secret: %s", output.String())
	}
}

func decodeT242AuditEvents(t *testing.T, data []byte) []AuditEvent {
	t.Helper()
	var events []AuditEvent
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event AuditEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode audit event: %v", err)
		}
		events = append(events, event)
	}
	return events
}
