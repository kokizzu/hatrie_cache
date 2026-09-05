package hatSql

import "testing"

var queryTraceSpansBenchmarkSink interface{}

func benchmarkQueryTraceRecorderWithEvents() *QueryTraceRecorder {
	recorder := NewQueryTraceRecorder(64)
	for index := 0; index < 64; index++ {
		recorder.ObserveSQLQuery(QueryEvent{
			QueryID:      "query-" + string(rune('a'+index%26)),
			ElapsedNanos: 10_000,
			OK:           true,
			Operators: []QueryOperator{
				{Node: "SCAN", InputRows: 10_000, OutputRows: 1_000, ElapsedNanos: 6_000},
				{Node: "FILTER", InputRows: 1_000, OutputRows: 100, ElapsedNanos: 2_000},
			},
		})
	}
	return recorder
}

func BenchmarkQueryTraceRecorderEvents(b *testing.B) {
	recorder := benchmarkQueryTraceRecorderWithEvents()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		queryTraceSpansBenchmarkSink = recorder.Events()
	}
}

func BenchmarkQueryTraceRecorderOpenTelemetrySpans(b *testing.B) {
	recorder := benchmarkQueryTraceRecorderWithEvents()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		queryTraceSpansBenchmarkSink = recorder.OpenTelemetrySpans()
	}
}
