package hatSql

import (
	"errors"
	"math"
	"testing"
)

func TestCHU26SQLQueryProfilerAggregatesBoundedOperatorMemory(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:                 2,
		MaxMemoryOperatorsPerQuery: 2,
	})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	if captured, err := profiler.RecordMemory("q1", "scan", SQLQueryMemorySample{AllocatedBytes: 100, PeakBytes: 80, RetainedBytes: 40}); err != nil || !captured {
		t.Fatalf("first RecordMemory() = %v/%v, want captured", captured, err)
	}
	if captured, err := profiler.RecordMemory("q1", "scan", SQLQueryMemorySample{AllocatedBytes: 200, PeakBytes: 120, RetainedBytes: 50}); err != nil || !captured {
		t.Fatalf("second RecordMemory() = %v/%v, want captured", captured, err)
	}
	if captured, err := profiler.RecordMemory("q1", "filter", SQLQueryMemorySample{AllocatedBytes: 30, PeakBytes: 20, RetainedBytes: 10}); err != nil || !captured {
		t.Fatalf("filter RecordMemory() = %v/%v, want captured", captured, err)
	}
	if captured, err := profiler.RecordMemory("q1", "project", SQLQueryMemorySample{AllocatedBytes: 10}); err != nil || captured {
		t.Fatalf("bounded RecordMemory() = %v/%v, want dropped without error", captured, err)
	}

	profile, ok := profiler.MemoryProfile("q1")
	if !ok || len(profile.Operators) != 2 || profile.DroppedObservations != 1 {
		t.Fatalf("MemoryProfile() = %#v/%v, want two operators and one dropped observation", profile, ok)
	}
	if profile.Operators[0].Operator != "filter" || profile.Operators[1].Operator != "scan" {
		t.Fatalf("MemoryProfile() operators = %#v, want deterministic order", profile.Operators)
	}
	scan := profile.Operators[1]
	if scan.Observations != 2 || scan.AllocatedBytes != 300 || scan.PeakBytes != 120 || scan.MaxRetainedBytes != 50 {
		t.Fatalf("scan memory = %#v, want aggregated values", scan)
	}
	profile.Operators[0].Operator = "mutated"
	copy, ok := profiler.MemoryProfile("q1")
	if !ok || copy.Operators[0].Operator != "filter" {
		t.Fatalf("MemoryProfile() was not isolated: %#v/%v", copy, ok)
	}
	stats := profiler.Stats()
	if stats.MemoryObservationCount != 3 || stats.DroppedMemoryObservationCount != 1 || stats.MemoryQueryCount != 1 {
		t.Fatalf("profiler memory stats = %#v", stats)
	}
}

func TestCHU26SQLQueryProfilerMemoryCountersSaturateAndValidate(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxMemoryOperatorsPerQuery: 1})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	if _, err := profiler.RecordMemory("q1", "scan", SQLQueryMemorySample{AllocatedBytes: math.MaxUint64}); err != nil {
		t.Fatal(err)
	}
	if _, err := profiler.RecordMemory("q1", "scan", SQLQueryMemorySample{AllocatedBytes: 1}); err != nil {
		t.Fatal(err)
	}
	profile, ok := profiler.MemoryProfile("q1")
	if !ok || profile.Operators[0].AllocatedBytes != math.MaxUint64 {
		t.Fatalf("saturated memory profile = %#v/%v", profile, ok)
	}
	if _, err := profiler.RecordMemory("", "scan", SQLQueryMemorySample{}); !errors.Is(err, ErrSQLQueryProfilerQueryIDRequired) {
		t.Fatalf("blank query ID error = %v", err)
	}
	if _, err := profiler.RecordMemory("q1", "", SQLQueryMemorySample{}); !errors.Is(err, ErrSQLQueryProfilerOperatorRequired) {
		t.Fatalf("blank operator error = %v", err)
	}
	profiler.Close()
	if _, err := profiler.RecordMemory("q1", "scan", SQLQueryMemorySample{}); !errors.Is(err, ErrSQLQueryProfilerClosed) {
		t.Fatalf("closed profiler error = %v", err)
	}
}
