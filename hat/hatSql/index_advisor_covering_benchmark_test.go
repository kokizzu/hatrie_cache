package hatSql

import (
	"context"
	"testing"
	"time"
)

var sqlCoveringIndexAdvisorBenchmarkSink interface{}

func BenchmarkSQLIndexAdvisorCovering(b *testing.B) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "name": "Ada", "email": "ada@example.test"}}, nil
	})
	for _, benchmark := range []struct {
		name          string
		query         string
		advisor       bool
		coveringQuery bool
	}{
		{
			name:  "baseline",
			query: "FROM CACHE('people') AS person WHERE person.id = 1 SELECT person.email, person.name",
		},
		{
			name:    "advisor_noncovering",
			query:   "FROM CACHE('people') AS person WHERE person.id >= 1 SELECT person.name",
			advisor: true,
		},
		{
			name:          "advisor_covering",
			query:         "FROM CACHE('people') AS person WHERE person.id = 1 SELECT person.email, person.name",
			advisor:       true,
			coveringQuery: true,
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			var advisor *SQLIndexAdvisor
			options := QueryOptions{}
			if benchmark.advisor {
				advisor = NewSQLIndexAdvisor(8)
				options.SlowQueryThreshold = time.Nanosecond
				options.IndexAdvisor = advisor
			}
			b.ReportAllocs()
			for b.Loop() {
				result, err := ExecuteQueryParameters(context.Background(), benchmark.query, resolver, nil, options)
				if err != nil {
					b.Fatal(err)
				}
				sqlCoveringIndexAdvisorBenchmarkSink = result.Rows
			}
			if benchmark.coveringQuery {
				sqlCoveringIndexAdvisorBenchmarkSink = advisor.CoveringRecommendations()
			}
		})
	}
}

func BenchmarkSQLIndexAdvisorCoveringRecommendations(b *testing.B) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "name": "Ada", "email": "ada@example.test"}}, nil
	})
	if _, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE person.id = 1 SELECT person.email, person.name", resolver, nil, QueryOptions{
		SlowQueryThreshold: time.Nanosecond,
		IndexAdvisor:       advisor,
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		sqlCoveringIndexAdvisorBenchmarkSink = advisor.CoveringRecommendations()
	}
}
