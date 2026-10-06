package hatSql

import "testing"

var c235TaskProfilerBenchmarkSink SQLTaskProfile

func BenchmarkC235TaskProfilerRecord(b *testing.B) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 8})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLTaskProfileSample{Operation: SQLTaskRead, Table: "orders", Part: "part-01", Column: "customer_id", Rows: 10, Bytes: 1200, ElapsedNanos: 40}
	if _, err := profiler.Record(sample); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if captured, err := profiler.Record(sample); err != nil || !captured {
			b.Fatalf("Record() = %v/%v", captured, err)
		}
	}
	b.StopTimer()
	profiles := profiler.Profiles()
	c235TaskProfilerBenchmarkSink = profiles[0]
}
