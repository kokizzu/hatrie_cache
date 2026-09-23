package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM218MaterializedViewPlanner(b *testing.B) {
	queries := map[string]string{
		"rare":   "FROM CACHE('people') AS p WHERE p.region = 'rare' SELECT p.id, p.region, p.payload",
		"common": "FROM CACHE('people') AS p WHERE p.region = 'common' SELECT p.id, p.region, p.payload",
	}
	for name, query := range queries {
		b.Run("source_scan_"+name, func(b *testing.B) {
			resolver := m218BenchmarkResolver()
			b.ReportAllocs()
			for range b.N {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{})
				if err != nil || len(result.Rows) == 0 {
					b.Fatalf("source query result = %d rows, error = %v", len(result.Rows), err)
				}
			}
		})

		b.Run("arrangement_scan_"+name, func(b *testing.B) {
			resolver := m218BenchmarkResolver()
			views := m218BenchmarkViews(b, resolver, false)
			options := hatSql.QueryOptions{ProjectionCatalog: views}
			b.ReportAllocs()
			for range b.N {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
				if err != nil || len(result.Rows) == 0 {
					b.Fatalf("arrangement query result = %d rows, error = %v", len(result.Rows), err)
				}
			}
		})

		b.Run("point_lookup_"+name, func(b *testing.B) {
			resolver := m218BenchmarkResolver()
			views := m218BenchmarkViews(b, resolver, true)
			options := hatSql.QueryOptions{ProjectionCatalog: views}
			b.ReportAllocs()
			for range b.N {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
				if err != nil || len(result.Rows) == 0 {
					b.Fatalf("point query result = %d rows, error = %v", len(result.Rows), err)
				}
			}
		})
	}
}

func m218BenchmarkResolver() *m218PointPlannerResolver {
	rows := make([]hatSql.Row, 10000)
	for index := range rows {
		region := "common"
		if index%100 == 0 {
			region = "rare"
		}
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"region":  region,
			"payload": "payload-" + strconv.Itoa(index),
		}
	}
	return &m218PointPlannerResolver{rows: rows, version: "1"}
}

func m218BenchmarkViews(b *testing.B, resolver *m218PointPlannerResolver, point bool) *hatSql.MaterializedViews {
	b.Helper()
	views := hatSql.NewMaterializedViews()
	definition := hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.payload",
		Dependencies: []string{"people"},
	}
	if point {
		definition.PointLookupFields = []string{"region"}
	}
	if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views
}
