package hatAudit

import "testing"

func BenchmarkAuditLoggerDefaultLog(b *testing.B) {
	logger := NewAuditLogger(nil)
	event := AuditEvent{Action: "command", Command: "GET", Key: "tenant-a:people", OK: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := logger.Log(event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuditLoggerSampledLog(b *testing.B) {
	logger, err := NewAuditLoggerWithOptions(nil, AuditLoggerOptions{SuccessSampleRate: 0.1})
	if err != nil {
		b.Fatal(err)
	}
	event := AuditEvent{Action: "command", Command: "GET", Key: "tenant-a:people", OK: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := logger.Log(event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuditLoggerSinkLog(b *testing.B) {
	logger, err := NewAuditLoggerWithOptions(nil, AuditLoggerOptions{
		Sinks: []AuditSink{AuditSinkFunc(func(AuditEvent) error { return nil })},
	})
	if err != nil {
		b.Fatal(err)
	}
	event := AuditEvent{Action: "command", Command: "GET", Key: "tenant-a:people", OK: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := logger.Log(event); err != nil {
			b.Fatal(err)
		}
	}
}
