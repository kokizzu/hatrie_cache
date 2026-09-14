package hatCache

import "testing"

func TestAuditAliasesExposeSamplingAndSinks(t *testing.T) {
	seen := 0
	logger, err := NewAuditLoggerWithOptions(nil, AuditLoggerOptions{
		SuccessSampleRate: 1,
		Sinks: []AuditSink{AuditSinkFunc(func(AuditEvent) error {
			seen++
			return nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Log(AuditEvent{Action: "alias", OK: true}); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("sink calls = %d, want 1", seen)
	}
}
