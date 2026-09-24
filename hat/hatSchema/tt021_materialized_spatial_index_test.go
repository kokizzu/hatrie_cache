package hatSchema

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT021MaterializedSourceSpatialIndexUsesCandidatesAndMaintainsRows(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Indexed: true},
		{Name: "latitude"},
		{Name: "longitude"},
		{Name: "kind"},
	})
	for _, row := range []Row{
		{"id": "jakarta", "latitude": -6.2088, "longitude": 106.8456, "kind": "city"},
		{"id": "bandung", "latitude": -6.9175, "longitude": 107.6191, "kind": "city"},
		{"id": "singapore", "latitude": 1.3521, "longitude": 103.8198, "kind": "city"},
		{"id": "null-point", "latitude": nil, "longitude": nil, "kind": "unknown"},
		{"id": "invalid-point", "latitude": 999.0, "longitude": 0.0, "kind": "unknown"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatalf("Insert(%q): %v", row["id"], err)
		}
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"points": source}}
	predicate := hatSql.SQLGeoPredicate{
		Kind:           hatSql.SQLGeoPredicateWithinBox,
		LatitudeField:  "latitude",
		LongitudeField: "longitude",
		Bounds:         hatSql.GeoBoundingBox{MinLatitude: -10, MaxLatitude: 0, MinLongitude: 100, MaxLongitude: 110},
	}
	if _, available, err := adapter.ResolveSQLGeoSource("CACHE", "points", predicate); err != nil || available {
		t.Fatalf("ResolveSQLGeoSource before build = available %v, err %v; want unavailable", available, err)
	}

	baseline, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -10, 0, 100, 110) AND p.kind = 'city'
SELECT p.id ORDER BY p.id`, adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("baseline query: %v", err)
	}
	wantBaseline := []hatSql.Row{{"id": "bandung"}, {"id": "jakarta"}}
	if !reflect.DeepEqual(baseline.Rows, wantBaseline) {
		t.Fatalf("baseline rows = %#v, want %#v", baseline.Rows, wantBaseline)
	}

	report, err := source.BuildSpatialIndex("latitude", "longitude")
	if err != nil {
		t.Fatalf("BuildSpatialIndex: %v", err)
	}
	if report.Rows != 5 || report.Attempts != 1 {
		t.Fatalf("BuildSpatialIndex report = %#v, want five rows and one attempt", report)
	}
	candidates, available, err := adapter.ResolveSQLGeoSource("CACHE", "points", predicate)
	if err != nil || !available {
		t.Fatalf("ResolveSQLGeoSource after build = %#v/%v/%v, want available", candidates, available, err)
	}
	candidateIDs := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		candidateIDs[candidate["id"].(string)] = true
	}
	for _, id := range []string{"jakarta", "bandung", "null-point", "invalid-point"} {
		if !candidateIDs[id] {
			t.Fatalf("candidate set omitted %q: %#v", id, candidateIDs)
		}
	}
	if candidateIDs["singapore"] {
		t.Fatalf("candidate set included out-of-box Singapore: %#v", candidateIDs)
	}

	if _, err := source.Insert(Row{"id": "bogor", "latitude": -6.595, "longitude": 106.816, "kind": "city"}); err != nil {
		t.Fatalf("post-build Insert: %v", err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatalf("BuildUniqueIndex: %v", err)
	}
	if _, err := source.Upsert(Row{"id": "bandung", "latitude": -6.9175, "longitude": 120.0, "kind": "city"}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict: func(_, incoming Row) (Row, error) {
			return incoming, nil
		},
	}); err != nil {
		t.Fatalf("post-build Upsert: %v", err)
	}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -10, 0, 100, 110) AND p.kind = 'city'
SELECT p.id ORDER BY p.id`, adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("indexed query: %v", err)
	}
	want := []hatSql.Row{{"id": "bogor"}, {"id": "jakarta"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed rows = %#v, want %#v", result.Rows, want)
	}
}

func TestTT021MaterializedSourceSpatialIndexPreservesRadiusAndDatelineQueries(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
	})
	for _, row := range []Row{
		{"id": "jakarta", "latitude": -6.2088, "longitude": 106.8456},
		{"id": "bandung", "latitude": -6.9175, "longitude": 107.6191},
		{"id": "singapore", "latitude": 1.3521, "longitude": 103.8198},
		{"id": "dateline-east", "latitude": 0.0, "longitude": 179.0},
		{"id": "dateline-west", "latitude": 0.0, "longitude": -179.0},
		{"id": "null-point", "latitude": nil, "longitude": nil},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatalf("Insert(%q): %v", row["id"], err)
		}
	}
	if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
		t.Fatalf("BuildSpatialIndex: %v", err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"points": source}}
	radius, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
SELECT p.id ORDER BY p.id`, adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("radius query: %v", err)
	}
	if want := []hatSql.Row{{"id": "bandung"}, {"id": "jakarta"}}; !reflect.DeepEqual(radius.Rows, want) {
		t.Fatalf("radius rows = %#v, want %#v", radius.Rows, want)
	}
	box, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -1, 1, 170, -170)
SELECT p.id ORDER BY p.id`, adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("dateline query: %v", err)
	}
	if want := []hatSql.Row{{"id": "dateline-east"}, {"id": "dateline-west"}}; !reflect.DeepEqual(box.Rows, want) {
		t.Fatalf("dateline rows = %#v, want %#v", box.Rows, want)
	}
}
