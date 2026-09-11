package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLSourceFrontierRequirementBaseline(b *testing.B) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "value": "ready"}}, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery("FROM CACHE('items') SELECT id", resolver)
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
		}
	}
}

func BenchmarkSQLSourceFrontierRequirementDisabled(b *testing.B) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, available: false}
	benchmarkSQLSourceFrontierRequirement(b, resolver, SQLQueryOptions{})
}

func BenchmarkSQLSourceFrontierRequirementEnabled(b *testing.B) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, frontier: 1, ready: true, available: true}
	benchmarkSQLSourceFrontierRequirement(b, resolver, mz007FrontierOptions(1))
}

func benchmarkSQLSourceFrontierRequirement(b *testing.B, resolver *mz007FrontierResolver, options SQLQueryOptions) {
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options)
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("ExecuteSQLQueryContext() = %#v, %v", result, err)
		}
	}
}
