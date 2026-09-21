package hatSql

import (
	"strconv"
	"testing"
	"time"
)

var ch234StageRecordSink bool
var ch234StageProfileSink SQLQueryStageProfile

func BenchmarkCH234StageProfilerRecord(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:        1,
		MaxStagesPerQuery: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryStageSample{
		Stage:          "scan",
		CPUTime:        2 * time.Microsecond,
		BlockedTime:    time.Microsecond,
		Rows:           100,
		Bytes:          4096,
		AllocatedBytes: 512,
		PeakBytes:      256,
		RetainedBytes:  128,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch234StageRecordSink, _ = profiler.RecordStage("query", sample)
	}
}

func BenchmarkCH234StageProfilerSnapshot(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:        1,
		MaxStagesPerQuery: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		accepted, recordErr := profiler.RecordStage("query", SQLQueryStageSample{
			Stage: strconv.Itoa(i),
		})
		if recordErr != nil || !accepted {
			b.Fatalf("seed stage %d: accepted=%v err=%v", i, accepted, recordErr)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch234StageProfileSink, _ = profiler.StageProfile("query")
	}
}
