package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type t026BenchmarkResolver struct {
	rows []Row
}

func (resolver *t026BenchmarkResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return resolver.rows, nil
}

func (resolver *t026BenchmarkResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error) {
	if name != "CACHE" || key != "people" || field != "region" || value != "sg" {
		return nil, false, nil
	}
	return resolver.rows[:32], true, nil
}

func (resolver *t026BenchmarkResolver) ResolveSQLNamedIndexedSource(name, key, index, field string, value interface{}) ([]Row, bool, error) {
	if name != "CACHE" || key != "people" || index != "region_copy" || field != "region" || value != "sg" {
		return nil, false, nil
	}
	return resolver.rows[:32], true, nil
}

func newT026BenchmarkResolver() *t026BenchmarkResolver {
	rows := make([]Row, 128)
	for index := range rows {
		region := "id"
		if index < 32 {
			region = "sg"
		}
		rows[index] = Row{"id": strconv.Itoa(index), "region": region}
	}
	return &t026BenchmarkResolver{rows: rows}
}

const t026BenchmarkQuery = "FROM CACHE('people') AS person WHERE person.region = 'sg' SELECT person.id"

func BenchmarkT026DefaultQuery(b *testing.B) {
	resolver := newT026BenchmarkResolver()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), t026BenchmarkQuery, resolver, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 32 {
			b.Fatalf("default result rows=%d err=%v", len(result.Rows), err)
		}
	}
}

func BenchmarkT026FieldHint(b *testing.B) {
	resolver := newT026BenchmarkResolver()
	options := SQLQueryOptions{IndexHint: SQLIndexHint{Source: "person", Field: "region", Mode: SQLIndexHintForce}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), t026BenchmarkQuery, resolver, options)
		if err != nil || len(result.Rows) != 32 {
			b.Fatalf("field hint result rows=%d err=%v", len(result.Rows), err)
		}
	}
}
