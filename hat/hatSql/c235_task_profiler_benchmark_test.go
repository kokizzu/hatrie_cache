package hatSql

import (
	"testing"
	"time"
)

func BenchmarkC235BaselineQueryProfilerRecord(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxQueries: 1, MaxSamplesPerQuery: 64})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryProfileSample{Operator: "scan", Rows: 32, Bytes: 4096, Timestamp: time.Unix(1, 0)}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := profiler.Record("query-1", sample); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC235SQLTaskProfilerRecord(b *testing.B) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	record := SQLTaskProfileRecord{
		Table:     "events",
		Part:      "part-0001",
		Column:    "user_id",
		Operation: SQLTaskRead,
		Rows:      32,
		Bytes:     4096,
	}
	if _, err := profiler.Record(record); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := profiler.Record(record); err != nil {
			b.Fatal(err)
		}
	}
}
