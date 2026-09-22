package hatSql

import (
	"context"
	"testing"
)

func TestC234QueryProfilerObservesStageBytesAndAllocations(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxSamplesPerQuery: 8})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	inputBytes := 64
	outputBytes := 32
	profiler.ObserveSQLQuery(QueryEvent{
		QueryID:        "c234-query",
		AllocatedBytes: 123,
		HeapAllocBytes: 456,
		Operators: []QueryOperator{{
			Node:         "SCAN",
			OutputRows:   3,
			ElapsedNanos: 700,
			InputBytes:   &inputBytes,
			OutputBytes:  &outputBytes,
		}},
	})

	profile, ok := profiler.Profile("c234-query")
	if !ok || len(profile.Samples) != 1 {
		t.Fatalf("Profile() = %#v, %t; want one stage sample", profile, ok)
	}
	sample := profile.Samples[0]
	if sample.Operator != "SCAN" || sample.ElapsedTime != 700 || sample.Rows != 3 || sample.Bytes != 96 {
		t.Fatalf("stage sample = %#v, want SCAN/700ns/3 rows/96 bytes", sample)
	}
	memory, ok := profiler.MemoryProfile("c234-query")
	if !ok || len(memory.Operators) != 1 {
		t.Fatalf("MemoryProfile() = %#v, %t; want one query allocation profile", memory, ok)
	}
	allocation := memory.Operators[0]
	if allocation.Operator != sqlQueryProfilerQueryMemoryOperator || allocation.AllocatedBytes != 123 || allocation.MaxHeapAllocBytes != 456 {
		t.Fatalf("allocation profile = %#v, want query allocation sample", allocation)
	}
}

func TestC234QueryOptionsProfilerCapturesExecution(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('events') AS events SELECT events.id, events.value WHERE events.value >= 0 ORDER BY events.id LIMIT 8", c234QueryProfilerBenchmarkResolverValue, nil, SQLQueryOptions{
		QueryID:  "c234-execution",
		Profiler: profiler,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 8 {
		t.Fatalf("Execute() rows = %d, want 8", len(result.Rows))
	}
	profile, ok := profiler.Profile("c234-execution")
	if !ok || len(profile.Samples) == 0 {
		t.Fatalf("Profile() = %#v, %t; want execution stages", profile, ok)
	}
	memory, ok := profiler.MemoryProfile("c234-execution")
	if !ok || len(memory.Operators) != 1 || memory.Operators[0].AllocatedBytes == 0 {
		t.Fatalf("MemoryProfile() = %#v, %t; want non-zero query allocation", memory, ok)
	}
}

func TestC234QueryOptionsProfilerCapturesStreamingExecution(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	rows := 0
	err = ExecuteSQLQueryRows(context.Background(), "FROM CACHE('events') AS events SELECT events.id, events.value WHERE events.value >= 0 ORDER BY events.id LIMIT 8", c234QueryProfilerBenchmarkResolverValue, nil, SQLQueryOptions{
		QueryID:  "c234-stream",
		Profiler: profiler,
	}, func([]string, SQLRow) error {
		rows++
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if rows != 8 {
		t.Fatalf("ExecuteSQLQueryRows() rows = %d, want 8", rows)
	}
	profile, ok := profiler.Profile("c234-stream")
	if !ok || len(profile.Samples) != 1 || profile.Samples[0].Operator != "STREAM OUTPUT" {
		t.Fatalf("Profile() = %#v, %t; want stream output stage", profile, ok)
	}
	memory, ok := profiler.MemoryProfile("c234-stream")
	if !ok || len(memory.Operators) != 1 || memory.Operators[0].AllocatedBytes == 0 {
		t.Fatalf("MemoryProfile() = %#v, %t; want non-zero stream allocation", memory, ok)
	}
}

func TestC234QueryProfilerIgnoresIncompleteEvents(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	profiler.ObserveSQLQuery(QueryEvent{Operators: []QueryOperator{{Node: "SCAN"}}})
	if stats := profiler.Stats(); stats.QueryCount != 0 || stats.SampleCount != 0 || stats.MemoryObservationCount != 0 {
		t.Fatalf("Stats() after incomplete event = %#v, want empty", stats)
	}
}
