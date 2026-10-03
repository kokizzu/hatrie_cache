package hatSchema

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func newT025MaterializedSource(t *testing.T) *MaterializedSource {
	t.Helper()
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
		{Name: "kind"},
	})
	for _, row := range []Row{
		{"id": "jakarta", "latitude": -6.2088, "longitude": 106.8456, "kind": "city"},
		{"id": "bandung", "latitude": -6.9175, "longitude": 107.6191, "kind": "city"},
		{"id": "singapore", "latitude": 1.3521, "longitude": 103.8198, "kind": "city"},
		{"id": "null-point", "latitude": nil, "longitude": nil, "kind": "unknown"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	return source
}

func TestT025MaterializedSourceBuildsAndMaintainsRTreeIndex(t *testing.T) {
	source := newT025MaterializedSource(t)
	report, err := source.BuildRTreeIndex("geo", RTreeIndexOptions{
		LatitudeField:  "latitude",
		LongitudeField: "longitude",
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Name != "geo" || report.Rows != 4 || report.Attempts != 1 {
		t.Fatalf("BuildRTreeIndex() report = %#v", report)
	}
	if !source.HasIndex("geo") {
		t.Fatal("HasIndex(geo) = false after R-tree build")
	}

	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"points": source}}
	query := `FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
  AND p.kind = 'city'
SELECT p.id ORDER BY p.id`
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "bandung"}, {"id": "jakarta"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed radius rows = %#v, want %#v", result.Rows, want)
	}

	if _, err := source.Insert(Row{"id": "depok", "latitude": -6.4025, "longitude": 106.7942, "kind": "city"}); err != nil {
		t.Fatal(err)
	}
	result, err = hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "bandung"}, {"id": "depok"}, {"id": "jakarta"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("maintained radius rows = %#v, want %#v", result.Rows, want)
	}

	if !source.DropIndex("geo") || source.DropIndex("geo") || source.HasIndex("geo") {
		t.Fatal("DropIndex(geo) did not remove the R-tree")
	}
}

func TestT025MaterializedSourceRTreeRejectsInvalidDefinitions(t *testing.T) {
	source := newT025MaterializedSource(t)
	for name, options := range map[string]RTreeIndexOptions{
		"missing latitude":  {LatitudeField: "missing", LongitudeField: "longitude"},
		"missing longitude": {LatitudeField: "latitude", LongitudeField: "missing"},
		"same field":        {LatitudeField: "latitude", LongitudeField: "latitude"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := source.BuildRTreeIndex(name, options); err == nil {
				t.Fatal("BuildRTreeIndex() unexpectedly succeeded")
			}
		})
	}
}
