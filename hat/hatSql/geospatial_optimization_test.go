package hatSql

import (
	"reflect"
	"strconv"
	"testing"
)

func TestGeoIndexWithinBoxDoesNotDuplicateDatelineCandidates(t *testing.T) {
	index, err := NewGeoIndex(1)
	if err != nil {
		t.Fatal(err)
	}
	for id, point := range map[string]GeoPoint{
		"east":   {Latitude: 0, Longitude: 179.5},
		"west":   {Latitude: 0, Longitude: -179.5},
		"inside": {Latitude: 0, Longitude: 0},
	} {
		if err := index.Upsert(id, point); err != nil {
			t.Fatal(err)
		}
	}
	got, err := index.WithinBox(GeoBoundingBox{MinLatitude: -1, MaxLatitude: 1, MinLongitude: 170, MaxLongitude: -170})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"east", "west"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WithinBox() = %#v, want %#v", got, want)
	}

	coarse, err := NewGeoIndex(360)
	if err != nil {
		t.Fatal(err)
	}
	for id, point := range map[string]GeoPoint{
		"east": {Latitude: 0, Longitude: 179.5},
		"west": {Latitude: 0, Longitude: -179.5},
	} {
		if err := coarse.Upsert(id, point); err != nil {
			t.Fatal(err)
		}
	}
	got, err = coarse.WithinBox(GeoBoundingBox{MinLatitude: -1, MaxLatitude: 1, MinLongitude: 170, MaxLongitude: -170})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("coarse WithinBox() = %#v, want %#v", got, want)
	}
}

func BenchmarkGeoIndexWithinBoxCandidateCollection(b *testing.B) {
	index, err := NewGeoIndex(0.25)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < 10_000; value++ {
		if err := index.Upsert(strconv.Itoa(value), GeoPoint{Latitude: -10 + float64(value%200)/10, Longitude: 100 + float64(value/200)/10}); err != nil {
			b.Fatal(err)
		}
	}
	bounds := GeoBoundingBox{MinLatitude: -1, MaxLatitude: 1, MinLongitude: 101, MaxLongitude: 103}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := index.WithinBox(bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGeoIndexWithinBoxSparseWide(b *testing.B) {
	index, err := NewGeoIndex(1)
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Upsert("origin", GeoPoint{}); err != nil {
		b.Fatal(err)
	}
	bounds := GeoBoundingBox{MinLatitude: -90, MaxLatitude: 90, MinLongitude: -180, MaxLongitude: 180}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := index.WithinBox(bounds); err != nil {
			b.Fatal(err)
		}
	}
}
