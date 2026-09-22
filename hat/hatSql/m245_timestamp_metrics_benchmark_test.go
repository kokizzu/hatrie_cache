package hatSql

import "testing"

var m245TimestampTelemetrySink *SQLTelemetry

func BenchmarkM245QueryTelemetryObserve(b *testing.B) {
	telemetry := NewSQLTelemetry()
	event := SQLQueryEvent{ElapsedNanos: 1000, ResultBytes: 64, OK: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		telemetry.ObserveSQLQuery(event)
	}
	m245TimestampTelemetrySink = telemetry
}

func BenchmarkM245TimestampTelemetryObserve(b *testing.B) {
	telemetry := NewSQLTelemetry()
	event := SQLTimestampTelemetryEvent{
		InputTimestamp:   42,
		OutputTimestamp:  45,
		Updates:          7,
		Batches:          1,
		InputAtUnixNano:  1_000_000_000,
		OutputAtUnixNano: 1_250_000_000,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		telemetry.ObserveSQLTimestamp(event)
	}
	m245TimestampTelemetrySink = telemetry
}
