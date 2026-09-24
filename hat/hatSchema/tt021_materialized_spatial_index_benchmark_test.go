package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var tt021MaterializedSpatialSink int

func BenchmarkTT021MaterializedSpatialScan(b *testing.B) {
	benchmarkTT021MaterializedSpatialQuery(b, false)
}

func BenchmarkTT021MaterializedSpatialIndex(b *testing.B) {
	benchmarkTT021MaterializedSpatialQuery(b, true)
}

func BenchmarkTT021MaterializedSpatialIndexBuild(b *testing.B) {
	source := benchmarkTT021MaterializedSpatialSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		report, err := source.BuildSpatialIndex("latitude", "longitude")
		if err != nil {
			b.Fatal(err)
		}
		tt021MaterializedSpatialSink += report.IndexedRows
	}
}

func BenchmarkTT021MaterializedSpatialIndexMarshal(b *testing.B) {
	source := benchmarkTT021MaterializedSpatialSource(b)
	if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
		b.Fatal(err)
	}
	wire, err := source.MarshalSpatialIndex("latitude", "longitude")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := source.MarshalSpatialIndex("latitude", "longitude"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire-bytes")
}

func BenchmarkTT021MaterializedSpatialIndexRestore(b *testing.B) {
	indexed := benchmarkTT021MaterializedSpatialSource(b)
	if _, err := indexed.BuildSpatialIndex("latitude", "longitude"); err != nil {
		b.Fatal(err)
	}
	wire, err := indexed.MarshalSpatialIndex("latitude", "longitude")
	if err != nil {
		b.Fatal(err)
	}
	source := benchmarkTT021MaterializedSpatialSource(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for b.Loop() {
		if err := source.RestoreSpatialIndex("latitude", "longitude", wire); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire-bytes")
}

func BenchmarkTT021MaterializedSpatialUpsertScan(b *testing.B) {
	benchmarkTT021MaterializedSpatialUpsert(b, false)
}

func BenchmarkTT021MaterializedSpatialUpsertIndex(b *testing.B) {
	benchmarkTT021MaterializedSpatialUpsert(b, true)
}

func benchmarkTT021MaterializedSpatialQuery(b *testing.B, indexed bool) {
	source := benchmarkTT021MaterializedSpatialSource(b)
	if indexed {
		if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
			b.Fatal(err)
		}
	}
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"points": source}}
	const query = `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -1, 1, -180, -170)
SELECT p.id`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		tt021MaterializedSpatialSink += len(result.Rows)
	}
}

func benchmarkTT021MaterializedSpatialSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "latitude"}, {Name: "longitude"}})
	for rowID := 0; rowID < 20000; rowID++ {
		latitude := float64((rowID%160)-80) + 0.25
		longitude := float64((rowID/160)%360) - 180 + 0.25
		if _, err := source.Insert(Row{"id": rowID, "latitude": latitude, "longitude": longitude}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}

func benchmarkTT021MaterializedSpatialUpsert(b *testing.B, indexed bool) {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "latitude"}, {Name: "longitude"}})
	if _, err := source.Insert(Row{"id": "row", "latitude": 0.0, "longitude": 0.0}); err != nil {
		b.Fatal(err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		b.Fatal(err)
	}
	if indexed {
		if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := source.Upsert(Row{
			"id":        "row",
			"latitude":  float64(iteration%90) - 45,
			"longitude": float64(iteration%180) - 90,
		}, MaterializedUpsertOptions{
			ConflictField: "id",
			OnConflict: func(_, incoming Row) (Row, error) {
				return incoming, nil
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		if !result.Updated {
			b.Fatal("upsert did not update the existing row")
		}
	}
}
