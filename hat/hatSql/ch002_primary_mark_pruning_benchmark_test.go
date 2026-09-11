package hatSql

import (
	"context"
	"testing"
)

var ch002PrimaryMarkBenchmarkSink SQLQueryResult
var ch002PrimaryMarkStreamBenchmarkSink int

func BenchmarkSQLOrderedRangeBaseline(b *testing.B) {
	resolver := &ch002LegacyOrderedResolver{rows: ch002PrimaryMarkRows(100_000)}
	query := ch002PrimaryMarkQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch002PrimaryMarkBenchmarkSink = result
	}
}

func BenchmarkSQLOrderedRangeSparseMarks(b *testing.B) {
	resolver := &ch002SparseMarkResolver{ch002LegacyOrderedResolver: ch002LegacyOrderedResolver{rows: ch002PrimaryMarkRows(100_000)}}
	query := ch002PrimaryMarkQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch002PrimaryMarkBenchmarkSink = result
	}
}

func BenchmarkSQLOrderedRangeStreamBaseline(b *testing.B) {
	resolver := &ch002LegacyOrderedResolver{rows: ch002PrimaryMarkRows(100_000)}
	query := ch002PrimaryMarkQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ch002PrimaryMarkStreamBenchmarkSink = count
	}
}

func BenchmarkSQLOrderedRangeStreamSparseMarks(b *testing.B) {
	resolver := &ch002SparseMarkResolver{ch002LegacyOrderedResolver: ch002LegacyOrderedResolver{rows: ch002PrimaryMarkRows(100_000)}}
	query := ch002PrimaryMarkQuery()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		ch002PrimaryMarkStreamBenchmarkSink = count
	}
}
