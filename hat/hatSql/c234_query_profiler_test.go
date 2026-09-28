package hatSql

import (
	"context"
	"testing"
)

type c234RowsResolver struct {
	rows []Row
}

func (resolver c234RowsResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func TestC234QueryProfilerCapturesExecutionStagesAndAllocations(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxSamplesPerQuery: 16, CaptureAllocations: true})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}

	query := "FROM CACHE('events') AS event SELECT event.kind"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, c234RowsResolver{
		rows: []Row{{"kind": "queued"}, {"kind": "done"}},
	}, nil, SQLQueryOptions{QueryID: "c234-query", QueryProfiler: profiler})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}

	profile, ok := profiler.Profile("c234-query")
	if !ok {
		t.Fatal("profiler did not retain the query")
	}
	if len(profile.Samples) == 0 {
		t.Fatalf("profile samples = %#v, want at least one execution stage", profile)
	}
	for _, sample := range profile.Samples {
		if sample.Operator == "" || sample.ElapsedTime <= 0 || sample.Rows == 0 {
			t.Fatalf("invalid stage sample = %#v", sample)
		}
	}
	if profile.AllocatedBytes == 0 || profile.AllocationCount == 0 {
		t.Fatalf("allocation summary = %#v, want query-boundary allocation counters", profile)
	}
	if profile.ResultBytes == 0 {
		t.Fatalf("profile result bytes = %#v, want stage/result byte accounting", profile)
	}
}

func TestC234QueryProfilerAllocationCaptureIsOptIn(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	query := "FROM CACHE('events') AS event SELECT event.kind"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, c234RowsResolver{
		rows: []Row{{"kind": "queued"}},
	}, nil, SQLQueryOptions{QueryID: "c234-no-allocations", QueryProfiler: profiler}); err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	profile, ok := profiler.Profile("c234-no-allocations")
	if !ok {
		t.Fatal("profiler did not retain the query")
	}
	if profile.AllocatedBytes != 0 || profile.AllocationCount != 0 || profile.HeapBytes != 0 {
		t.Fatalf("allocation fields = %#v, want zero when capture is disabled", profile)
	}
}
