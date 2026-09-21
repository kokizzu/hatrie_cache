package hatSql

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCH235PartColumnProfilerRecord(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxPartColumnsPerQuery: 64})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryPartColumnSample{
		Part:      "part-1",
		Column:    "status",
		Operation: "read",
		CPUTime:   time.Microsecond,
		Rows:      10,
		Bytes:     100,
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if captured, err := profiler.RecordPartColumn("q1", sample); err != nil || !captured {
			b.Fatalf("RecordPartColumn() = %v/%v", captured, err)
		}
	}
}

func BenchmarkCH235PartColumnProfilerSnapshot(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxPartColumnsPerQuery: 64})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := profiler.RecordPartColumn("q1", SQLQueryPartColumnSample{
			Part:      "part-1",
			Column:    fmt.Sprintf("column-%02d", index),
			Operation: "read",
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if profile, ok := profiler.PartColumnProfile("q1"); !ok || len(profile.Entries) != 64 {
			b.Fatalf("PartColumnProfile() = %#v/%v", profile, ok)
		}
	}
}
