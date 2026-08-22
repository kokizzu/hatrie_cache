package hatAudit_test

import (
	"bytes"
	"testing"

	"hatrie_cache/hat/hatAudit"
)

func TestAuditLoggerIsUsableByImporters(t *testing.T) {
	var out bytes.Buffer
	logger := hatAudit.NewAuditLogger(&out)
	if err := logger.Log(hatAudit.AuditEvent{Action: "backup", OK: true}); err != nil {
		t.Fatalf("Log() error = %v", err)
	}
	if len(logger.Recent(1)) != 1 {
		t.Fatalf("Recent(1) length = %d, want 1", len(logger.Recent(1)))
	}
}
