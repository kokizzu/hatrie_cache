package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func newTR027TestSource(t *testing.T) *hatSql.RTreeSpatialSource {
	t.Helper()
	source, err := hatSql.NewRTreeSpatialSource(hatSql.RTreeSpatialSourceOptions{
		SourceName:     "CACHE",
		Name:           "points",
		LatitudeField:  "latitude",
		LongitudeField: "longitude",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := []hatSql.Row{
		{"id": "jakarta", "latitude": -6.2088, "longitude": 106.8456, "kind": "city"},
		{"id": "bandung", "latitude": -6.9175, "longitude": 107.6191, "kind": "city"},
		{"id": "singapore", "latitude": 1.3521, "longitude": 103.8198, "kind": "city"},
		{"id": "dateline-east", "latitude": 0.0, "longitude": 179.0, "kind": "border"},
		{"id": "dateline-west", "latitude": 0.0, "longitude": -179.0, "kind": "border"},
		{"id": "null-point", "latitude": nil, "longitude": nil, "kind": "unknown"},
	}
	for _, row := range rows {
		if err := source.Upsert(row["id"].(string), row); err != nil {
			t.Fatal(err)
		}
	}
	return source
}

func TestTR027RTreeSpatialSourceUsesIndexAndPreservesPredicates(t *testing.T) {
	source := newTR027TestSource(t)
	query := `FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
  AND p.kind = 'city'
SELECT p.id ORDER BY p.id`
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, source, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "bandung"}, {"id": "jakarta"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("radius rows = %#v, want %#v", result.Rows, want)
	}
	stats := source.Stats()
	if stats.SpatialQueries != 1 || stats.FullScans != 0 || stats.CandidateRows >= 5 {
		t.Fatalf("spatial stats = %#v, want one indexed query without full scan", stats)
	}

	box, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -1, 1, 170, -170)
SELECT p.id ORDER BY p.id`, source, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "dateline-east"}, {"id": "dateline-west"}}; !reflect.DeepEqual(box.Rows, want) {
		t.Fatalf("dateline rows = %#v, want %#v", box.Rows, want)
	}

	if err := source.Upsert("jakarta", hatSql.Row{"id": "jakarta", "latitude": 40.0, "longitude": 40.0, "kind": "city"}); err != nil {
		t.Fatal(err)
	}
	if deleted := source.Delete("singapore"); !deleted {
		t.Fatal("Delete(singapore) = false")
	}
	updated, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, source, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "bandung"}}; !reflect.DeepEqual(updated.Rows, want) {
		t.Fatalf("updated radius rows = %#v, want %#v", updated.Rows, want)
	}
}

func TestTR027SpatialSourceKeepsUnindexableRowsVisibleToSQL(t *testing.T) {
	source := newTR027TestSource(t)
	if err := source.Upsert("bad-point", hatSql.Row{"id": "bad-point", "latitude": "invalid", "longitude": 0.0}); err != nil {
		t.Fatal(err)
	}
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, 0, 0, 1000)
SELECT p.id`, source, hatSql.SQLQueryOptions{})
	if err == nil {
		t.Fatal("query with an invalid coordinate unexpectedly succeeded")
	}
}
