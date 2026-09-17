package hatSql

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCH017ProjectionAdvisor(b *testing.B) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"team": "blue", "points": int64(5), "region": "apac"}}, nil
	})
	advisor := NewSQLProjectionAdvisor(4)
	options := QueryOptions{
		ProjectionAdvisor:  advisor,
		QueryID:            "team_totals",
		SlowQueryThreshold: time.Nanosecond,
	}
	query := `FROM CACHE('events') AS e SELECT e.team, SUM(e.points) AS total WHERE e.region = 'apac' GROUP BY e.team ORDER BY e.team DESC`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}
