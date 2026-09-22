package hatSql

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestC233QueryCPUTimeBudgetCancelsCooperatively(t *testing.T) {
	rows := make([]Row, 100_000)
	for index := range rows {
		rows[index] = Row{"id": int64(index)}
	}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id", SourceResolverFunc(func(_, _ string) ([]Row, error) {
		return rows, nil
	}), SQLQueryOptions{
		DisableNativeDataflow: true,
		MaxCPUTime:            time.Nanosecond,
	})
	if !errors.Is(err, ErrSQLQueryCPUTimeExceeded) {
		t.Fatalf("query error = %v, want CPU-time budget error", err)
	}
}

func TestC233NegativeQueryCPUTimeBudgetIsRejected(t *testing.T) {
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT 1", nil, SQLQueryOptions{MaxCPUTime: -time.Nanosecond})
	if err == nil || !strings.Contains(err.Error(), "budgets cannot be negative") {
		t.Fatalf("negative MaxCPUTime error = %v, want budget validation error", err)
	}
}

func TestC233ZeroQueryCPUTimeBudgetPreservesResults(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id", SourceResolverFunc(func(_, _ string) ([]Row, error) {
		return []Row{{"id": int64(1)}, {"id": int64(2)}}, nil
	}), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("zero MaxCPUTime query error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("zero MaxCPUTime rows = %#v, want two rows", result.Rows)
	}
}

var c233QueryCPUBenchmarkSink SQLQueryResult

func BenchmarkC233QueryCPUTimeBudget(b *testing.B) {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "value": strconv.Itoa(index)}
	}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	for _, benchmark := range []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "disabled", options: SQLQueryOptions{DisableNativeDataflow: true}},
		{name: "enabled", options: SQLQueryOptions{DisableNativeDataflow: true, MaxCPUTime: time.Hour}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id, event.value", resolver, benchmark.options)
				if err != nil {
					b.Fatal(err)
				}
				c233QueryCPUBenchmarkSink = result
			}
		})
	}
}
