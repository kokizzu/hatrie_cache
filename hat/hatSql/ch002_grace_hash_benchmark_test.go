package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCH002HashJoinBaseline(b *testing.B) {
	resolver := newCH002StreamResolver(4096)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := hatSql.SQLQueryOptions{MaxRows: 100000}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 4096 {
			b.Fatalf("hash join rows = %d, err = %v", len(result.Rows), err)
		}
	}
}

func BenchmarkCH002GraceHashJoin(b *testing.B) {
	resolver := newCH002StreamResolver(4096)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := hatSql.SQLQueryOptions{
		MaxRows:        100000,
		MaxJoinBytes:   4 << 10,
		SpillDirectory: b.TempDir(),
		MaxSpillBytes:  64 << 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 4096 {
			b.Fatalf("grace hash join rows = %d, err = %v", len(result.Rows), err)
		}
	}
}

type ch002StreamResolver struct {
	left  []hatSql.Row
	right []hatSql.Row
}

func newCH002StreamResolver(count int) *ch002StreamResolver {
	left := make([]hatSql.Row, count)
	right := make([]hatSql.Row, count)
	for index := range count {
		key := int64(index)
		left[index] = hatSql.Row{"id": key, "k": key, "payload": "payload"}
		right[index] = hatSql.Row{"id": key, "k": key, "payload": "payload"}
	}
	return &ch002StreamResolver{left: left, right: right}
}

func (r *ch002StreamResolver) source(key string) []hatSql.Row {
	if key == "left" {
		return r.left
	}
	return r.right
}

func (r *ch002StreamResolver) ResolveSQLSource(_ string, key string) ([]hatSql.Row, error) {
	return r.source(key), nil
}

func (r *ch002StreamResolver) StreamSQLSource(ctx context.Context, _ string, key string, visit func(hatSql.Row) error) error {
	for _, row := range r.source(key) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}
