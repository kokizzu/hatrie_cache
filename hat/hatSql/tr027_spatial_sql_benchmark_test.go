package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type tr027ScanResolver struct {
	rows []SQLRow
}

func (resolver *tr027ScanResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "points" {
		return nil, nil
	}
	return resolver.rows, nil
}

func newTR027ScanResolver() *tr027ScanResolver {
	rows := make([]SQLRow, 50000)
	for index := range rows {
		rows[index] = SQLRow{
			"id":        strconv.Itoa(index),
			"latitude":  float64(-60 + index%120),
			"longitude": float64(-170 + (index/120)%340),
		}
	}
	return &tr027ScanResolver{rows: rows}
}

func BenchmarkTR027SpatialQueryBaseline(b *testing.B) {
	resolver := newTR027ScanResolver()
	query := `FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, 0, 0, 100000)
SELECT p.id`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("baseline query returned no rows")
		}
	}
}

func newTR027RTreeSource(b *testing.B) *RTreeSpatialSource {
	b.Helper()
	resolver := newTR027ScanResolver()
	source, err := NewRTreeSpatialSource(RTreeSpatialSourceOptions{
		SourceName:     "CACHE",
		Name:           "points",
		LatitudeField:  "latitude",
		LongitudeField: "longitude",
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, row := range resolver.rows {
		if err := source.Upsert(row["id"].(string), row); err != nil {
			b.Fatal(err)
		}
	}
	return source
}

func BenchmarkTR027SpatialQueryRTree(b *testing.B) {
	source := newTR027RTreeSource(b)
	query := `FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, 0, 0, 100000)
SELECT p.id`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, source, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("R-tree query returned no rows")
		}
	}
}

func BenchmarkTR027SpatialIndexBuild(b *testing.B) {
	resolver := newTR027ScanResolver()
	b.ReportAllocs()
	for range b.N {
		source, err := NewRTreeSpatialSource(RTreeSpatialSourceOptions{
			SourceName:     "CACHE",
			Name:           "points",
			LatitudeField:  "latitude",
			LongitudeField: "longitude",
		})
		if err != nil {
			b.Fatal(err)
		}
		for _, row := range resolver.rows {
			if err := source.Upsert(row["id"].(string), row); err != nil {
				b.Fatal(err)
			}
		}
	}
}
