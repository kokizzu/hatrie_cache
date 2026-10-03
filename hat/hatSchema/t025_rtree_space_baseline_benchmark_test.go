package hatSchema

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func newT025BaselineSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
	})
	for index := 0; index < 50000; index++ {
		if _, err := source.Insert(Row{
			"id":        strconv.Itoa(index),
			"latitude":  float64(-60 + index%120),
			"longitude": float64(-170 + (index/120)%340),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}

func BenchmarkT025MaterializedGeoQueryBaseline(b *testing.B) {
	source := newT025BaselineSource(b)
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"points": source}}
	query := `FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, 0, 0, 100000)
SELECT p.id`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("baseline query returned no rows")
		}
	}
}

func BenchmarkT025MaterializedGeoInsertBaseline(b *testing.B) {
	source := newT025BaselineSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 50000; index < 50000+b.N; index++ {
		if _, err := source.Insert(Row{
			"id":        strconv.Itoa(index),
			"latitude":  float64(-60 + index%120),
			"longitude": float64(-170 + (index/120)%340),
		}); err != nil {
			b.Fatal(err)
		}
	}
}
