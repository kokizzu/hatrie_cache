package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var sqlQueryManagerBenchmarkResult hatSql.QueryResult

func BenchmarkSQLQueryManager(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	b.Run("direct", func(b *testing.B) {
		for range b.N {
			result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlQueryManagerBenchmarkResult = result
		}
	})
	b.Run("managed", func(b *testing.B) {
		manager := hatSql.NewSQLQueryManager(256)
		for range b.N {
			result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlQueryManagerBenchmarkResult = result
		}
	})
}
