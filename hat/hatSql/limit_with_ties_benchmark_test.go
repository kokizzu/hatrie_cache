package hatSql

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkSQLLimitWithTies(b *testing.B) {
	values := make([]string, 100)
	for index := range values {
		score := 1000 - index
		values[index] = "(" + strconv.Itoa(index) + ", " + strconv.Itoa(score) + ")"
	}
	base := "FROM VALUES " + strings.Join(values, ", ") + " AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 10"
	withTies := base + " WITH TIES"
	for _, benchmark := range []struct {
		name  string
		query string
	}{
		{name: "ordinary-limit", query: base},
		{name: "limit-with-ties-no-tie", query: withTies},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), benchmark.query, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 10 {
					b.Fatalf("rows = %d, want 10", len(result.Rows))
				}
			}
		})
	}
}
