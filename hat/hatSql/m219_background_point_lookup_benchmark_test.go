package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func m219BenchmarkViews(b *testing.B) *hatSql.MaterializedViews {
	b.Helper()
	rows := make([]hatSql.Row, 10000)
	for rowIndex := range rows {
		rows[rowIndex] = hatSql.Row{
			"id":   int64(rowIndex),
			"name": "person-" + strconv.Itoa(rowIndex),
		}
	}
	source := &m219BackgroundSource{rows: rows}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}, source, hatSql.QueryOptions{}); err != nil {
		b.Fatalf("Create() error = %v", err)
	}
	return views
}

func m219BenchmarkPointLookupDefinition() hatSql.MaterializedViewPointLookupDefinition {
	return m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
		return strconv.FormatInt(row["id"].(int64), 10), nil
	})
}

func BenchmarkM219PointLookupBuild(b *testing.B) {
	b.Run("synchronous_publish", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			views := m219BenchmarkViews(b)
			definition := m219BenchmarkPointLookupDefinition()
			b.StartTimer()
			if err := views.CreatePointLookupIndex(definition); err != nil {
				b.Fatalf("CreatePointLookupIndex() error = %v", err)
			}
			b.StopTimer()
		}
	})

	b.Run("background_start", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			views := m219BenchmarkViews(b)
			release := make(chan struct{})
			definition := m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
				<-release
				return strconv.FormatInt(row["id"].(int64), 10), nil
			})
			b.StartTimer()
			build, err := views.StartPointLookupIndexBuild(context.Background(), definition)
			b.StopTimer()
			close(release)
			if err != nil {
				b.Fatalf("StartPointLookupIndexBuild() error = %v", err)
			}
			if _, err := build.Wait(context.Background()); err != nil {
				b.Fatalf("Wait() error = %v", err)
			}
		}
	})

	b.Run("background_total", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			views := m219BenchmarkViews(b)
			definition := m219BenchmarkPointLookupDefinition()
			b.StartTimer()
			build, err := views.StartPointLookupIndexBuild(context.Background(), definition)
			if err == nil {
				_, err = build.Wait(context.Background())
			}
			b.StopTimer()
			if err != nil {
				b.Fatalf("background build error = %v", err)
			}
		}
	})
}
