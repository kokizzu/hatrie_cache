package hatSql

import (
	"io"
	"testing"
)

func BenchmarkQueryTraceRecorderObserve(b *testing.B) {
	recorder := NewQueryTraceRecorder(1024)
	event := QueryEvent{QueryID: "query-1", Operators: []QueryOperator{{Node: "FILTER", InputRows: 1000, OutputRows: 10, ElapsedNanos: 500}}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		recorder.ObserveSQLQuery(event)
	}
}

func BenchmarkQueryTraceRecorderWriteJSONL(b *testing.B) {
	recorder := NewQueryTraceRecorder(1)
	recorder.ObserveSQLQuery(QueryEvent{QueryID: "query-1", Operators: []QueryOperator{{Node: "FILTER", InputRows: 1000, OutputRows: 10, ElapsedNanos: 500}}})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := recorder.WriteJSONL(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}
