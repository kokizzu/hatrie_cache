package hatSql

import (
	"reflect"
	"testing"
)

func TestGeoIndexBoundingBoxAndReplacement(t *testing.T) {
	index, err := NewGeoIndex(1)
	if err != nil {
		t.Fatal(err)
	}
	for id, point := range map[string]GeoPoint{
		"jakarta":    {Latitude: -6.2088, Longitude: 106.8456},
		"bandung":    {Latitude: -6.9175, Longitude: 107.6191},
		"singapore":  {Latitude: 1.3521, Longitude: 103.8198},
		"dateline-e": {Latitude: 0, Longitude: 179.5},
		"dateline-w": {Latitude: 0, Longitude: -179.5},
	} {
		if err := index.Upsert(id, point); err != nil {
			t.Fatalf("Upsert(%q): %v", id, err)
		}
	}

	inside, err := index.WithinBox(GeoBoundingBox{MinLatitude: -7.1, MaxLatitude: -6, MinLongitude: 106.5, MaxLongitude: 108})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bandung", "jakarta"}; !reflect.DeepEqual(inside, want) {
		t.Fatalf("WithinBox() = %v, want %v", inside, want)
	}

	dateline, err := index.WithinBox(GeoBoundingBox{MinLatitude: -1, MaxLatitude: 1, MinLongitude: 170, MaxLongitude: -170})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"dateline-e", "dateline-w"}; !reflect.DeepEqual(dateline, want) {
		t.Fatalf("dateline WithinBox() = %v, want %v", dateline, want)
	}

	if err := index.Upsert("jakarta", GeoPoint{Latitude: 0, Longitude: 0}); err != nil {
		t.Fatal(err)
	}
	inside, err = index.WithinBox(GeoBoundingBox{MinLatitude: -7.1, MaxLatitude: -6, MinLongitude: 106.5, MaxLongitude: 108})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bandung"}; !reflect.DeepEqual(inside, want) {
		t.Fatalf("replacement WithinBox() = %v, want %v", inside, want)
	}
}

func TestGeoIndexDistancePredicateAndValidation(t *testing.T) {
	index, err := NewGeoIndex(0.5)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("jakarta", GeoPoint{Latitude: -6.2088, Longitude: 106.8456}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("bandung", GeoPoint{Latitude: -6.9175, Longitude: 107.6191}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert("singapore", GeoPoint{Latitude: 1.3521, Longitude: 103.8198}); err != nil {
		t.Fatal(err)
	}

	nearby, err := index.WithinRadius(GeoPoint{Latitude: -6.2088, Longitude: 106.8456}, 200_000)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bandung", "jakarta"}; !reflect.DeepEqual(nearby, want) {
		t.Fatalf("WithinRadius() = %v, want %v", nearby, want)
	}

	distance, err := GeoDistanceMeters(GeoPoint{Latitude: -6.2088, Longitude: 106.8456}, GeoPoint{Latitude: -6.9175, Longitude: 107.6191})
	if err != nil {
		t.Fatal(err)
	}
	if distance < 100_000 || distance > 150_000 {
		t.Fatalf("GeoDistanceMeters() = %.2f, want about 118km", distance)
	}

	if _, err := NewGeoIndex(0); err == nil {
		t.Fatal("NewGeoIndex accepted zero cell size")
	}
	if err := index.Upsert("bad", GeoPoint{Latitude: 91}); err == nil {
		t.Fatal("Upsert accepted invalid latitude")
	}
	if _, err := index.WithinRadius(GeoPoint{}, -1); err == nil {
		t.Fatal("WithinRadius accepted negative radius")
	}
	if _, err := index.WithinBox(GeoBoundingBox{MinLatitude: 2, MaxLatitude: 1}); err == nil {
		t.Fatal("WithinBox accepted inverted latitude bounds")
	}
}
