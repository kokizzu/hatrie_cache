package hatAudit

import (
	"bytes"
	"reflect"
	"testing"
)

func TestTT037RedactedAuditLoggerStripsSensitiveCommandMetadata(t *testing.T) {
	var output bytes.Buffer
	var exported []AuditEvent
	logger, err := NewAuditLoggerWithOptions(&output, AuditLoggerOptions{
		RedactSensitive: true,
		Sinks: []AuditSink{AuditSinkFunc(func(event AuditEvent) error {
			exported = append(exported, event)
			return nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := AuditEvent{
		Time:    "2026-09-21T00:00:00Z",
		Action:  "command",
		Command: "SET",
		Key:     "customer@example.test",
		Path:    "/v1/command?token=secret-token",
		Message: "password=secret-password",
		Details: map[string]interface{}{"value": "secret-value", "rows": 3},
		OK:      true,
	}
	if err := logger.Log(input); err != nil {
		t.Fatal(err)
	}
	got := logger.Recent(1)
	if len(got) != 1 {
		t.Fatalf("Recent() returned %d events, want 1", len(got))
	}
	if got[0].Key != AuditRedactedValue || got[0].Message != AuditRedactedValue || got[0].Path != "/v1/command" || got[0].Details != nil {
		t.Fatalf("redacted event = %#v", got[0])
	}
	if len(exported) != 1 || !reflect.DeepEqual(exported[0], got[0]) {
		t.Fatalf("exported event = %#v, recent = %#v", exported, got)
	}
	if bytes.Contains(output.Bytes(), []byte("secret")) {
		t.Fatalf("audit output contains sensitive data: %q", output.String())
	}
	if input.Key != "customer@example.test" || input.Message != "password=secret-password" || input.Details["value"] != "secret-value" {
		t.Fatalf("Log mutated caller event: %#v", input)
	}
}

func TestTT037DefaultAuditLoggerPreservesExistingMetadata(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	input := AuditEvent{
		Time:    "2026-09-21T00:00:00Z",
		Action:  "command",
		Command: "GET",
		Key:     "customer@example.test",
		Path:    "/v1/command?token=legacy",
		Message: "legacy-message",
		Details: map[string]interface{}{"rows": 3},
		OK:      true,
	}
	if err := logger.Log(input); err != nil {
		t.Fatal(err)
	}
	got := logger.Recent(1)[0]
	if got.Key != input.Key || got.Path != input.Path || got.Message != input.Message || !reflect.DeepEqual(got.Details, input.Details) {
		t.Fatalf("default logger changed metadata: got %#v want %#v", got, input)
	}
	if !bytes.Contains(output.Bytes(), []byte("customer@example.test")) {
		t.Fatalf("default logger did not preserve legacy output: %q", output.String())
	}
}

func TestTT037RedactedAuditEventIsIdempotent(t *testing.T) {
	input := AuditEvent{
		Key:     "secret-key",
		Path:    "/command?secret=value",
		Message: "secret-message",
		Details: map[string]interface{}{"secret": "value"},
	}
	first := RedactAuditEvent(input)
	second := RedactAuditEvent(first)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("redaction is not idempotent: first=%#v second=%#v", first, second)
	}
}
