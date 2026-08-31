package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const incrementalProjectionBenchmarkBatchSize = 32

func BenchmarkIncrementalProjectionCoalescedRefresh(b *testing.B) {
	b.Run("refresh_each_change", func(b *testing.B) {
		views, resolver := newIncrementalProjectionBenchmarkViews(b)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for change := 0; change < incrementalProjectionBenchmarkBatchSize; change++ {
				if _, err := views.RefreshChanged(context.Background(), []string{"events"}, resolver, hatSql.QueryOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("coalesced_journal_batch", func(b *testing.B) {
		views, resolver := newIncrementalProjectionBenchmarkViews(b)
		runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "events", Enabled: true})
		if err != nil {
			b.Fatal(err)
		}
		changes := make([]hatSql.ProjectionChange, incrementalProjectionBenchmarkBatchSize)
		sequence := uint64(0)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index := range changes {
				sequence++
				changes[index] = hatSql.ProjectionChange{Sequence: sequence, Dependency: "events"}
			}
			if _, err := runner.Apply(context.Background(), changes); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func newIncrementalProjectionBenchmarkViews(b *testing.B) (*hatSql.MaterializedViews, hatSql.SourceResolver) {
	b.Helper()
	rows := make([]hatSql.Row, 10_000)
	for index := range rows {
		rows[index] = hatSql.Row{"team": string(rune('a' + index%10)), "score": index % 100}
	}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if key != "events" {
			return nil, nil
		}
		return hatSql.CloneRows(rows), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_totals",
		Query:        "FROM CACHE('events') SELECT team, COUNT(*) AS total, SUM(score) AS score_total GROUP BY team",
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views, resolver
}
