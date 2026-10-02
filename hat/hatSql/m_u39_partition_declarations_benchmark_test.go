package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu039BenchmarkResolver struct{}

func (mu039BenchmarkResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return []hatSql.Row{{"id": int64(1), "created_at": int64(20)}}, nil
}

func BenchmarkMU039ExplainWithoutDeclaration(b *testing.B) {
	resolver := mu039BenchmarkResolver{}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') SELECT id ORDER BY created_at DESC", resolver); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU039ExplainWithDeclaration(b *testing.B) {
	resolver := hatSql.CatalogResolver{
		Source:  mu039BenchmarkResolver{},
		Catalog: hatSql.Catalog{Partitions: []hatSql.SQLPartitionDeclaration{mu039Declaration()}},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') SELECT id ORDER BY created_at DESC", resolver); err != nil {
			b.Fatal(err)
		}
	}
}
