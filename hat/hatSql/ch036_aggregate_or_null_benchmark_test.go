package hatSql_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var ch036AggregateOrNullBenchmarkSink hatSql.SQLQueryResult

func BenchmarkCH036AggregateBaseline(b *testing.B) {
	query := ch036AggregateOrNullBenchmarkQuery("SUM(events.value)")
	compiled, err := hatSql.CompileSQLQuery(query)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := compiled.Execute(context.Background(), nil, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch036AggregateOrNullBenchmarkSink = result
	}
}

func BenchmarkCH036AggregateOrNull(b *testing.B) {
	query := ch036AggregateOrNullBenchmarkQuery("SUM_OR_NULL(events.value)")
	compiled, err := hatSql.CompileSQLQuery(query)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := compiled.Execute(context.Background(), nil, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch036AggregateOrNullBenchmarkSink = result
	}
}

func ch036AggregateOrNullBenchmarkQuery(aggregate string) string {
	var source strings.Builder
	source.WriteString("FROM VALUES ")
	for i := 0; i < 4096; i++ {
		if i > 0 {
			source.WriteString(", ")
		}
		source.WriteByte('(')
		source.WriteString(strconv.Itoa(i % 97))
		source.WriteByte(')')
	}
	source.WriteString(" AS events(value) SELECT ")
	source.WriteString(aggregate)
	source.WriteString(" AS value")
	return source.String()
}
