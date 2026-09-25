package hatSql

import (
	"context"
	"testing"
)

var benchmarkCH041OnePassGroupingSink SQLQueryResult

func benchmarkCH041GroupingQuery(b *testing.B) *sqlQuery {
	b.Helper()
	query, err := parseSQLQuery(ch041GroupingSetQuery)
	if err != nil {
		b.Fatalf("parse CUBE query: %v", err)
	}
	if query.groupingSetsTemplate == nil {
		b.Fatal("CUBE query did not retain the grouping-sets template")
	}
	return query
}

func benchmarkCH041GroupingExecution(b *testing.B, legacy bool) {
	query := benchmarkCH041GroupingQuery(b)
	if legacy {
		query.groupingSetsTemplate = nil
	}
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		b.Fatalf("create execution control: %v", err)
	}
	b.Cleanup(cancel)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := executeSQLQueryWithMetrics(query, nil, nil, nil, control)
		if err != nil || len(result.Rows) == 0 {
			b.Fatalf("execute CUBE query: err=%v rows=%d", err, len(result.Rows))
		}
		benchmarkCH041OnePassGroupingSink = result
	}
}

func BenchmarkCH041GroupingSetQueryOnePass(b *testing.B) {
	benchmarkCH041GroupingExecution(b, false)
}

func BenchmarkCH041GroupingSetQueryExpandedUnionAll(b *testing.B) {
	benchmarkCH041GroupingExecution(b, true)
}
