package hatSql

import (
	"context"
	"testing"
)

func TestC234SQLQueryProfilerRecordsStageAllocationCounters(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	if captured, err := profiler.Record("q1", SQLQueryProfileSample{
		Operator:         "scan",
		Rows:             128,
		Bytes:            4096,
		AllocatedBytes:   2048,
		AllocatedObjects: 12,
	}); err != nil || !captured {
		t.Fatalf("Record() = %v/%v, want captured", captured, err)
	}
	profile, ok := profiler.Profile("q1")
	if !ok || len(profile.Samples) != 1 {
		t.Fatalf("Profile() = %#v/%v, want one sample", profile, ok)
	}
	sample := profile.Samples[0]
	if sample.AllocatedBytes != 2048 || sample.AllocatedObjects != 12 {
		t.Fatalf("allocation counters = %d/%d, want 2048/12", sample.AllocatedBytes, sample.AllocatedObjects)
	}
}

func TestC234SQLQueryEventAllocationCountersAreOptIn(t *testing.T) {
	var profiled SQLQueryEvent
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1) AS values(id) SELECT id", nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			profiled = event
		}),
		ProfileAllocations: true,
	}); err != nil {
		t.Fatalf("profiled query error = %v", err)
	}
	if profiled.AllocatedBytes == 0 || profiled.AllocatedObjects == 0 {
		t.Fatalf("profiled allocation counters = %d/%d, want nonzero", profiled.AllocatedBytes, profiled.AllocatedObjects)
	}

	var ordinary SQLQueryEvent
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1) AS values(id) SELECT id", nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			ordinary = event
		}),
	}); err != nil {
		t.Fatalf("ordinary query error = %v", err)
	}
	if ordinary.AllocatedBytes != 0 || ordinary.AllocatedObjects != 0 {
		t.Fatalf("ordinary allocation counters = %d/%d, want zero", ordinary.AllocatedBytes, ordinary.AllocatedObjects)
	}
}
