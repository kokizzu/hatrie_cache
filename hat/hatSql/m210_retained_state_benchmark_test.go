package hatSql

import (
	"context"
	"testing"
)

type m210BenchmarkResolver struct {
	rows []Row
}

func (resolver m210BenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver m210BenchmarkResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	return resolver, nil, nil
}

var m210BenchmarkResult SQLQueryResult

func m210BenchmarkRows() []Row {
	return []Row{
		{"id": int64(1), "value": "Ada"},
		{"id": int64(2), "value": "Grace"},
		{"id": int64(3), "value": "Edsger"},
		{"id": int64(4), "value": "Barbara"},
	}
}

func newM210RetainedBenchmarkState(b *testing.B) *SQLRetainedState {
	b.Helper()
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 8})
	if err != nil {
		b.Fatal(err)
	}
	if err := state.Publish(1, []SQLRetainedStateSource{{Name: "CACHE", Key: "items", Rows: m210BenchmarkRows()}}); err != nil {
		b.Fatal(err)
	}
	return state
}

func BenchmarkM210RetainedStateAsOfQuery(b *testing.B) {
	state := newM210RetainedBenchmarkState(b)
	frontier := uint64(1)
	options := SQLQueryOptions{AsOfFrontier: &frontier}
	query := "FROM CACHE('items') SELECT id, value"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, state, options)
		if err != nil {
			b.Fatal(err)
		}
		m210BenchmarkResult = result
	}
}

func BenchmarkM210DirectProviderAsOfQuery(b *testing.B) {
	resolver := m210BenchmarkResolver{rows: m210BenchmarkRows()}
	frontier := uint64(1)
	options := SQLQueryOptions{AsOfFrontier: &frontier}
	query := "FROM CACHE('items') SELECT id, value"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		m210BenchmarkResult = result
	}
}

func BenchmarkM210RetainedStatePublish(b *testing.B) {
	state := newM210RetainedBenchmarkState(b)
	rows := m210BenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := state.Publish(uint64(i+2), []SQLRetainedStateSource{{Name: "CACHE", Key: "items", Rows: rows}}); err != nil {
			b.Fatal(err)
		}
	}
}
