package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu039BaselineResolver struct{}

func (mu039BaselineResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	return []hatSql.Row{{"region": "apac", "created_at": int64(2)}}, nil
}

var mu039BenchmarkSink hatSql.SQLQueryResult

func BenchmarkMU039ExplainBaseline(b *testing.B) {
	resolver := mu039BaselineResolver{}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region, created_at ORDER BY created_at DESC", resolver)
		if err != nil {
			b.Fatal(err)
		}
		mu039BenchmarkSink = result
	}
}

func BenchmarkMU039ExplainWithLayout(b *testing.B) {
	resolver := &mu039LayoutResolver{
		rows: []hatSql.Row{{"region": "apac", "created_at": int64(2)}},
		layout: hatSql.SQLSourceLayout{
			PartitionBy: []string{"region"},
			OrderBy:     []hatSql.SQLSourceOrder{{Field: "created_at", Descending: true}},
		},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region, created_at ORDER BY created_at DESC", resolver)
		if err != nil {
			b.Fatal(err)
		}
		mu039BenchmarkSink = result
	}
}
