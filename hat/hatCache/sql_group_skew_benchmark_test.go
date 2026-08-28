package hatCache

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func benchmarkSQLGroupSkewQuery() string {
	var builder strings.Builder
	builder.WriteString("FROM VALUES ")
	for index := 0; index < 512; index++ {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "(%d)", index%16)
	}
	builder.WriteString(" AS events(kind) GROUP BY kind SELECT kind, COUNT(*) AS total")
	return builder.String()
}

func BenchmarkSQLGroupSkewGuard(b *testing.B) {
	query := benchmarkSQLGroupSkewQuery()
	for _, benchmark := range []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "disabled"},
		{name: "enabled", options: SQLQueryOptions{MaxGroupRowsPerKey: 64}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, benchmark.options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
