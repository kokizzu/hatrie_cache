package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM223MaterializedViewHydrationStatus(b *testing.B) {
	views := m223ReadyMaterializedViews(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, ok := views.HydrationStatus("people_projection")
		if !ok || status.State != hatSql.MaterializedViewHydrationReady {
			b.Fatalf("HydrationStatus() = %#v/%v, want ready", status, ok)
		}
	}
}

func m223ReadyMaterializedViews(b *testing.B) *hatSql.MaterializedViews {
	b.Helper()
	views := hatSql.NewMaterializedViews()
	resolver := &m219PointBuildResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg"},
			{"id": int64(2), "region": "jp"},
		},
		version: "1",
	}
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_projection",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views
}
