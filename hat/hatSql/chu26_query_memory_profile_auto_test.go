package hatSql

import (
	"context"
	"testing"
)

const chu26AutomaticMemoryProfileQuery = `FROM VALUES ('b', 2), ('a', 1) AS src(group_id, value)
SELECT src.group_id, SUM(src.value) AS total
GROUP BY src.group_id
ORDER BY src.group_id`

func TestCHU26SQLQueryProfilerAutomaticallyCapturesOperatorMemory(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{CaptureOperatorMemory: true})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	result, err := ExecuteSQLQueryContext(context.Background(), chu26AutomaticMemoryProfileQuery, nil, SQLQueryOptions{QueryProfiler: profiler})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("result rows = %#v, want two rows", result.Rows)
	}
	profile, ok := profiler.MemoryProfile(result.QueryID)
	if !ok {
		t.Fatalf("MemoryProfile(%q) not found", result.QueryID)
	}
	profiles := make(map[string]SQLQueryOperatorMemoryProfile, len(profile.Operators))
	for _, operator := range profile.Operators {
		profiles[operator.Operator] = operator
	}
	for _, name := range []string{"GROUP BY", "SORT"} {
		operator, ok := profiles[name]
		if !ok {
			t.Fatalf("automatic memory profile = %#v, missing %q", profile.Operators, name)
		}
		if operator.Observations == 0 || operator.PeakBytes == 0 || operator.MaxRetainedBytes == 0 {
			t.Fatalf("automatic %q profile = %#v, want positive observation and bytes", name, operator)
		}
	}
}

func TestCHU26SQLQueryProfilerOperatorMemoryIsDisabledByDefault(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	result, err := ExecuteSQLQueryContext(context.Background(), chu26AutomaticMemoryProfileQuery, nil, SQLQueryOptions{QueryProfiler: profiler})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if _, ok := profiler.MemoryProfile(result.QueryID); ok {
		t.Fatalf("default profiler unexpectedly captured operator memory")
	}
}
