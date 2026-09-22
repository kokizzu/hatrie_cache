package hatSql

import (
	"context"
	"testing"
)

type m224HydrationProgressBenchmarkResolver struct {
	rows []Row
}

func (resolver m224HydrationProgressBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver m224HydrationProgressBenchmarkResolver) SQLSourceCardinality(string, string) (int, bool, bool, error) {
	return len(resolver.rows), true, true, nil
}

func (resolver m224HydrationProgressBenchmarkResolver) StreamSQLSource(ctx context.Context, _, _ string, visit func(Row) error) error {
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func (resolver m224HydrationProgressBenchmarkResolver) ResolveSQLSourceWithProgress(ctx context.Context, _, _ string, report func(Row) error) ([]Row, error) {
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := report(row); err != nil {
			return nil, err
		}
	}
	return resolver.rows, nil
}

func BenchmarkM224MaterializedViewHydrationProgress(b *testing.B) {
	definition := m223HydrationDefinition()
	rows := m223HydrationBenchmarkRows(256)
	resolver := m224HydrationProgressBenchmarkResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		views := NewMaterializedViews()
		if _, err := views.CreateCold(definition); err != nil {
			b.Fatal(err)
		}
		if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM224MaterializedViewHydrationProgressControl(b *testing.B) {
	definition := m223HydrationDefinition()
	rows := m223HydrationBenchmarkRows(256)
	resolver := &m223HydrationResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		views := NewMaterializedViews()
		if _, err := views.CreateCold(definition); err != nil {
			b.Fatal(err)
		}
		if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
