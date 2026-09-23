package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkM217MaterializedViewPointLookupIndexed(b *testing.B) {
	views := m217BenchmarkMaterializedViews(b)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int
	for iteration := 0; iteration < b.N; iteration++ {
		rows, available, err := views.PointLookup("people_view", "region", "region-042")
		if err != nil || !available {
			b.Fatalf("PointLookup() = %v, %v", available, err)
		}
		checksum += len(rows)
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "matched_rows")
}

func BenchmarkM217MaterializedViewPointLookupSnapshotScan(b *testing.B) {
	views := m217BenchmarkMaterializedViews(b)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int
	for iteration := 0; iteration < b.N; iteration++ {
		view, ok := views.Get("people_view")
		if !ok {
			b.Fatal("Get() did not find materialized view")
		}
		for _, row := range view.Result.Rows {
			if row["region"] == "region-042" {
				checksum++
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "matched_rows")
}

func BenchmarkM217MaterializedViewRefreshWithoutPointLookup(b *testing.B) {
	m217BenchmarkRefresh(b, nil)
}

func BenchmarkM217MaterializedViewRefreshWithPointLookup(b *testing.B) {
	m217BenchmarkRefresh(b, []string{"region"})
}

func m217BenchmarkRefresh(b *testing.B, fields []string) {
	views, resolver := m217BenchmarkMaterializedViewsWithFields(b, fields)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func m217BenchmarkMaterializedViews(b *testing.B) *MaterializedViews {
	views, _ := m217BenchmarkMaterializedViewsWithFields(b, []string{"region"})
	return views
}

func m217BenchmarkMaterializedViewsWithFields(b *testing.B, fields []string) (*MaterializedViews, SourceResolver) {
	b.Helper()
	rows := make([]Row, 20000)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"region": fmt.Sprintf("region-%03d", index%100),
			"name":   fmt.Sprintf("person-%05d", index),
		}
	}
	resolver := SourceResolverFunc(func(_ string, _ string) ([]Row, error) {
		return CloneRows(rows), nil
	})
	views := NewMaterializedViews()
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') SELECT id, region, name",
		Dependencies:      []string{"people"},
		PointLookupFields: fields,
	}, resolver, QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views, resolver
}
