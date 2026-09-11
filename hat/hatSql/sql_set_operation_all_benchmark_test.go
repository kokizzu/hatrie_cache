package hatSql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

var sqlSetOperationAllBenchmarkSink int

func benchmarkSQLSetOperationQuery(operation string) string {
	var query strings.Builder
	query.WriteString("FROM VALUES ")
	for index := 0; index < 1024; index++ {
		if index > 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(&query, "(%d)", index%256)
	}
	query.WriteString(" AS lhs(id) SELECT lhs.id ")
	query.WriteString(operation)
	query.WriteString(" FROM VALUES ")
	for index := 0; index < 1024; index++ {
		if index > 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(&query, "(%d)", index%512)
	}
	query.WriteString(" AS rhs(id) SELECT rhs.id")
	return query.String()
}

func BenchmarkSQLSetOperationAll(b *testing.B) {
	query, err := CompileSQLQuery(benchmarkSQLSetOperationQuery("INTERSECT"))
	if err != nil {
		b.Fatal(err)
	}
	intersectAllQuery, err := CompileSQLQuery(benchmarkSQLSetOperationQuery("INTERSECT ALL"))
	if err != nil {
		b.Fatal(err)
	}
	exceptAllQuery, err := CompileSQLQuery(benchmarkSQLSetOperationQuery("EXCEPT ALL"))
	if err != nil {
		b.Fatal(err)
	}
	b.Run("IntersectDistinct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlSetOperationAllBenchmarkSink += len(result.Rows)
		}
	})
	for _, test := range []struct {
		name  string
		query *CompiledSQLQuery
	}{
		{name: "IntersectAll", query: intersectAllQuery},
		{name: "ExceptAll", query: exceptAllQuery},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				result, err := test.query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				sqlSetOperationAllBenchmarkSink += len(result.Rows)
			}
		})
	}
}
