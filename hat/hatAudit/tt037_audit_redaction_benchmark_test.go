package hatAudit

import "testing"

func BenchmarkTT037AuditLoggerRedactedLog(b *testing.B) {
	logger := NewRedactedAuditLogger(nil)
	event := AuditEvent{
		Action:  "command",
		Command: "SET",
		Key:     "customer@example.test",
		Path:    "/v1/command?token=secret-token",
		Message: "password=secret-password",
		Details: map[string]interface{}{"value": "secret-value", "rows": 3},
		OK:      true,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := logger.Log(event); err != nil {
			b.Fatal(err)
		}
	}
}
