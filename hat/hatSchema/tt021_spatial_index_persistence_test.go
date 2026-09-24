package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT021SpatialIndexBinaryPersistenceRoundTripAndValidation(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "latitude": float64(10), "longitude": float64(20), "kind": "city"},
		{"id": int64(2), "latitude": nil, "longitude": float64(30), "kind": "unknown"},
		{"id": int64(3), "latitude": float64(-5), "longitude": float64(40), "kind": "city"},
	}
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
		{Name: "kind"},
	})
	for _, row := range rows {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
		t.Fatal(err)
	}
	wire, err := source.MarshalSpatialIndex("latitude", "longitude")
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) == 0 {
		t.Fatal("spatial index persistence frame is empty")
	}
	t.Logf("spatial index persistence frame bytes = %d", len(wire))

	restored := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
		{Name: "kind"},
	})
	for _, row := range rows {
		if _, err := restored.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreSpatialIndex("latitude", "longitude", wire); err != nil {
		t.Fatal(err)
	}
	if !restored.HasSpatialIndex("latitude", "longitude") {
		t.Fatal("restored spatial index is not active")
	}
	assertTT021SpatialIDs(t, restored, []int64{1, 3})
	if _, err := restored.Insert(Row{"id": int64(4), "latitude": float64(5), "longitude": float64(50), "kind": "city"}); err != nil {
		t.Fatal(err)
	}
	assertTT021SpatialIDs(t, restored, []int64{1, 3, 4})

	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 1
	if err := restored.RestoreSpatialIndex("latitude", "longitude", corrupted); err == nil {
		t.Fatal("corrupted spatial index frame restored without error")
	}
	if !restored.HasSpatialIndex("latitude", "longitude") {
		t.Fatal("corrupted restore replaced the existing spatial index")
	}

	drifted := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "latitude"},
		{Name: "longitude"},
		{Name: "kind"},
	})
	for _, row := range rows {
		if _, err := drifted.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := drifted.Insert(Row{"id": int64(4), "latitude": float64(1), "longitude": float64(2), "kind": "city"}); err != nil {
		t.Fatal(err)
	}
	if err := drifted.RestoreSpatialIndex("latitude", "longitude", wire); err == nil {
		t.Fatal("spatial index for a different source snapshot restored without error")
	}
}

func assertTT021SpatialIDs(t *testing.T, source *MaterializedSource, want []int64) {
	t.Helper()
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"places": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('places') AS p
WHERE GEO_WITHIN_BOX(p.latitude, p.longitude, -10, 20, 0, 60) AND p.kind = 'city'
SELECT p.id`, adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("spatial result rows = %d, want %d: %#v", len(result.Rows), len(want), result.Rows)
	}
	for index, row := range result.Rows {
		got, ok := row["id"].(int64)
		if !ok || got != want[index] {
			t.Fatalf("spatial result[%d] id = %#v, want %d", index, row["id"], want[index])
		}
	}
}
