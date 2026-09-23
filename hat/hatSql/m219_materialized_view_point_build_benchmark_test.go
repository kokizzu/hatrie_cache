package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM219MaterializedViewPointBuild(b *testing.B) {
	b.Run("create_with_synchronous_index", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resolver := m219BenchmarkResolver()
			views := hatSql.NewMaterializedViews()
			if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
				Name:              "people_projection",
				Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.payload",
				Dependencies:      []string{"people"},
				PointLookupFields: []string{"region"},
			}, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("create_without_index", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resolver := m219BenchmarkResolver()
			views := hatSql.NewMaterializedViews()
			if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
				Name:         "people_projection",
				Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.payload",
				Dependencies: []string{"people"},
			}, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("background_enqueue_only", func(b *testing.B) {
		resolver := m219BenchmarkResolver()
		views := hatSql.NewMaterializedViews()
		if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
			Name:         "people_projection",
			Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.payload",
			Dependencies: []string{"people"},
		}, resolver, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
		const queueCapacity = 100_000
		queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Capacity: queueCapacity, Workers: 0})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { queue.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			if index > 0 && index%queueCapacity == 0 {
				b.StopTimer()
				queue.Close()
				queue, err = hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Capacity: queueCapacity, Workers: 0})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
				ID:       "benchmark-enqueue-" + strconv.Itoa(index),
				ViewName: "people_projection",
				Fields:   []string{"region"},
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("background_total_build", func(b *testing.B) {
		resolver := m219BenchmarkResolver()
		views := hatSql.NewMaterializedViews()
		if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
			Name:         "people_projection",
			Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.payload",
			Dependencies: []string{"people"},
		}, resolver, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
		queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1, HistoryCapacity: 100_000})
		if err != nil {
			b.Fatal(err)
		}
		defer queue.Close()
		if err := queue.Start(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
				ID:       "benchmark-build-" + strconv.Itoa(index),
				ViewName: "people_projection",
				Fields:   []string{"region"},
			}); err != nil {
				b.Fatal(err)
			}
			if err := queue.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func m219BenchmarkResolver() *m219BenchmarkSourceResolver {
	rows := make([]hatSql.Row, 4096)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"region":  "region-" + strconv.Itoa(index%64),
			"payload": "payload-" + strconv.Itoa(index),
		}
	}
	return &m219BenchmarkSourceResolver{rows: rows}
}

type m219BenchmarkSourceResolver struct {
	rows []hatSql.Row
}

func (resolver *m219BenchmarkSourceResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}
