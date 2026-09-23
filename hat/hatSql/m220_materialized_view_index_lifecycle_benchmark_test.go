package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM220MaterializedViewIndexLifecycle(b *testing.B) {
	b.Run("point_lookup", func(b *testing.B) {
		views := m220BenchmarkViews(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows, available, err := views.PointLookup("people_view", "region", "region-7")
			if err != nil || !available || len(rows) == 0 {
				b.Fatalf("PointLookup() = %d rows, %v, %v", len(rows), available, err)
			}
		}
	})

	b.Run("point_lookup_parallel", func(b *testing.B) {
		views := m220BenchmarkViews(b)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rows, available, err := views.PointLookup("people_view", "region", "region-7")
				if err != nil || !available || len(rows) == 0 {
					b.Fatalf("PointLookup() = %d rows, %v, %v", len(rows), available, err)
				}
			}
		})
	})
}

func m220BenchmarkViews(b *testing.B) *hatSql.MaterializedViews {
	b.Helper()
	rows := make([]hatSql.Row, 4096)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":     int64(index),
			"region": "region-" + strconv.Itoa(index%64),
			"name":   "person-" + strconv.Itoa(index),
		}
	}
	resolver := &m220BenchmarkResolver{rows: rows, version: "1"}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views
}

type m220BenchmarkResolver struct {
	rows    []hatSql.Row
	version string
}

func (resolver *m220BenchmarkResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *m220BenchmarkResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "people" {
		return "", false, nil
	}
	return resolver.version, true, nil
}
