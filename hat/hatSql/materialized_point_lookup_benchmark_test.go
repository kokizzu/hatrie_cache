package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var (
	m217PointLookupResultSink hatSql.QueryResult
	m217PointLookupRowSink    hatSql.Row
)

func BenchmarkM217MaterializedViewFullSnapshotPointProbe(b *testing.B) {
	views, rows := newM217MaterializedViewBenchmarkFixture(b, false)
	const key = "id-09000"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		view, ok := views.Get("people_view")
		if !ok {
			b.Fatal("Get() did not find people_view")
		}
		for _, row := range view.Result.Rows {
			if row["id"] == key {
				m217PointLookupRowSink = row
				break
			}
		}
	}
	_ = rows
}

func BenchmarkM217MaterializedViewIndexedPointProbe(b *testing.B) {
	views, rows := newM217MaterializedViewBenchmarkFixture(b, true)
	const key = "id-09000"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, found, err := views.LookupPoint("people_by_id", key)
		if err != nil || !found {
			b.Fatalf("LookupPoint() = %#v, %v, %v", result, found, err)
		}
		m217PointLookupResultSink = result
	}
	_ = rows
}

func BenchmarkM217MaterializedViewRefreshBaseline(b *testing.B) {
	views, rows := newM217MaterializedViewBenchmarkFixture(b, false)
	resolver := m217MaterializedViewResolver(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows[0]["value"] = int64(index)
		if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM217MaterializedViewRefreshWithPointLookup(b *testing.B) {
	views, rows := newM217MaterializedViewBenchmarkFixture(b, true)
	resolver := m217MaterializedViewResolver(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows[0]["value"] = int64(index)
		if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func newM217MaterializedViewBenchmarkFixture(b *testing.B, withIndex bool) (*hatSql.MaterializedViews, []hatSql.Row) {
	b.Helper()
	rows := make([]hatSql.Row, 10000)
	for index := range rows {
		rows[index] = hatSql.Row{"id": fmt.Sprintf("id-%05d", index), "value": int64(index)}
	}
	resolver := m217MaterializedViewResolver(rows)
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, value",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	if withIndex {
		if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
			Name:     "people_by_id",
			ViewName: "people_view",
			Key: func(row hatSql.Row) (string, error) {
				return row["id"].(string), nil
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
	return views, rows
}

func m217MaterializedViewResolver(rows []hatSql.Row) hatSql.SourceResolver {
	return hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if key != "people" {
			return nil, fmt.Errorf("unknown source %q", key)
		}
		return hatSql.CloneRows(rows), nil
	})
}
