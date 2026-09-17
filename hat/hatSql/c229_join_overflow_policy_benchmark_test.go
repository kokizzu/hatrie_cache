package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type c229JoinBenchmarkResolver struct {
	left  []SQLRow
	right []SQLRow
}

func (resolver c229JoinBenchmarkResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	if name != "CACHE" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	switch key {
	case "left":
		return resolver.left, nil
	case "right":
		return resolver.right, nil
	default:
		return nil, fmt.Errorf("unexpected cache key %q", key)
	}
}

func (resolver c229JoinBenchmarkResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(SQLRow) error) error {
	rows, err := resolver.ResolveSQLSource(name, key)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkC229JoinOverflowPolicy(b *testing.B) {
	resolver := c229JoinBenchmarkResolver{
		left: []SQLRow{
			{"id": 1, "k": "a"},
			{"id": 2, "k": "b"},
			{"id": 3, "k": "a"},
			{"id": 4, "k": "c"},
		},
		right: []SQLRow{
			{"k": "a", "name": "Ada"},
			{"k": "b", "name": "Bea"},
			{"k": "a", "name": "Cia"},
			{"k": "c", "name": "Dan"},
		},
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.name"
	benchmarks := []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "auto-default"},
		{name: "reject-budget", options: SQLQueryOptions{
			JoinOverflowPolicy: SQLJoinOverflowReject,
			MaxJoinBytes:       1 << 20,
		}},
		{name: "spill-budget", options: SQLQueryOptions{
			JoinOverflowPolicy: SQLJoinOverflowSpill,
			MaxJoinBytes:       128,
			SpillDirectory:     b.TempDir(),
			MaxSpillBytes:      1 << 20,
		}},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, benchmark.options)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 6 {
					b.Fatalf("rows = %d, want 6", len(result.Rows))
				}
			}
		})
	}
}
