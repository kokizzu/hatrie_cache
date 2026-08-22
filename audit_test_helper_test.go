package hatriecache

import (
	"encoding/json"
	"strings"
	"testing"
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
