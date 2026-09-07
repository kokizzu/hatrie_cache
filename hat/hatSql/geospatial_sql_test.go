package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLSpatialPredicatesEvaluateDistanceRadiusAndDatelineBox(t *testing.T) {
	distance, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM VALUES (-6.2088, 106.8456) AS point(latitude, longitude)
SELECT GEO_DISTANCE_METERS(point.latitude, point.longitude, -6.2088, 106.8456) AS distance`, nil, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(distance.Rows) != 1 || distance.Rows[0]["distance"] != float64(0) {
		t.Fatalf("distance rows = %#v, want zero distance", distance.Rows)
	}

	nearby, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM VALUES (-6.2088, 106.8456), (1.3521, 103.8198) AS point(latitude, longitude)
WHERE GEO_WITHIN_RADIUS(point.latitude, point.longitude, -6.2088, 106.8456, 200000)
SELECT point.latitude`, nil, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nearby.Rows, []hatSql.Row{{"latitude": -6.2088}}) {
		t.Fatalf("radius rows = %#v, want Jakarta", nearby.Rows)
	}

	dateLine, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM VALUES (0, 179), (0, -179), (0, 0) AS point(latitude, longitude)
WHERE GEO_WITHIN_BOX(point.latitude, point.longitude, -1, 1, 170, -170)
SELECT point.longitude`, nil, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dateLine.Rows, []hatSql.Row{{"longitude": int64(179)}, {"longitude": int64(-179)}}) {
		t.Fatalf("dateline box rows = %#v, want both dateline points", dateLine.Rows)
	}
}

func TestSQLSpatialPredicatesPreserveNullAndRejectInvalidCoordinates(t *testing.T) {
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM VALUES (NULL, 0) AS point(latitude, longitude)
SELECT GEO_WITHIN_RADIUS(point.latitude, point.longitude, 0, 0, 1) AS inside`, nil, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"inside": nil}}) {
		t.Fatalf("NULL spatial result = %#v, want NULL", result.Rows)
	}
	if _, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM VALUES (91, 0) AS point(latitude, longitude)
WHERE GEO_WITHIN_RADIUS(point.latitude, point.longitude, 0, 0, 1)
SELECT point.latitude`, nil, nil, hatSql.QueryOptions{}); err == nil {
		t.Fatal("invalid spatial coordinate was accepted")
	}
}
