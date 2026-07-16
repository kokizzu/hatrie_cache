package hatriecache

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func auditEventsFromJSONL(t *testing.T, data string) []AuditEvent {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(data), "\n")
	events := make([]AuditEvent, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event AuditEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("audit event JSON %q error = %v", line, err)
		}
		events = append(events, event)
	}
	return events
}

func TestAuditLoggerRetainsRecentEventsNewestFirst(t *testing.T) {
	var out bytes.Buffer
	logger := NewAuditLogger(&out)
	now := time.Date(2026, 7, 17, 4, 0, 0, 0, time.UTC)
	logger.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}

	if err := logger.Log(AuditEvent{Action: "first", OK: true}); err != nil {
		t.Fatalf("Log(first) error = %v", err)
	}
	if err := logger.Log(AuditEvent{Action: "second", OK: false}); err != nil {
		t.Fatalf("Log(second) error = %v", err)
	}

	recent := logger.Recent(2)
	if len(recent) != 2 || recent[0].Action != "second" || recent[1].Action != "first" {
		t.Fatalf("Recent(2) = %#v, want newest first", recent)
	}
	if recent[0].Time == "" || recent[1].Time == "" {
		t.Fatalf("Recent events = %#v, want timestamps populated", recent)
	}
}
