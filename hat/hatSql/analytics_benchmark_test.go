package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkGeoIndexWithinRadius(b *testing.B) {
	index, err := NewGeoIndex(0.25)
	if err != nil {
		b.Fatal(err)
	}
	for value := 0; value < 10_000; value++ {
		if err := index.Upsert(strconv.Itoa(value), GeoPoint{Latitude: -10 + float64(value%200)/10, Longitude: 100 + float64(value/200)/10}); err != nil {
			b.Fatal(err)
		}
	}
	center := GeoPoint{Latitude: 0, Longitude: 102}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := index.WithinRadius(center, 100_000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyGraphTraverse(b *testing.B) {
	graph := NewKeyGraph()
	for value := 0; value < 10_000; value++ {
		if err := graph.Link(strconv.Itoa(value), strconv.Itoa((value+1)%10_000)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := graph.Traverse("0", 128); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMatchOrderedEventSequence(b *testing.B) {
	events := make([]OrderedEvent, 10_000)
	start := time.Unix(0, 0).UTC()
	for index := range events {
		events[index] = OrderedEvent{Key: strconv.Itoa(index % 100), Kind: []string{"view", "add", "purchase"}[index%3], At: start.Add(time.Duration(index) * time.Second)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := MatchOrderedEventSequence(events, []string{"view", "add", "purchase"}, 5*time.Minute); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinOverlappingIntervals(b *testing.B) {
	start := time.Unix(0, 0).UTC()
	left, right := make([]IntervalRecord, 1_000), make([]IntervalRecord, 1_000)
	for index := range left {
		left[index] = IntervalRecord{Key: strconv.Itoa(index % 10), ID: "l" + strconv.Itoa(index), Interval: TimeInterval{Start: start.Add(time.Duration(index) * time.Minute), End: start.Add(time.Duration(index+3) * time.Minute)}}
		right[index] = IntervalRecord{Key: strconv.Itoa(index % 10), ID: "r" + strconv.Itoa(index), Interval: TimeInterval{Start: start.Add(time.Duration(index+1) * time.Minute), End: start.Add(time.Duration(index+4) * time.Minute)}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := JoinOverlappingIntervals(left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTimeBucketRollupAdd(b *testing.B) {
	rollup, err := NewTimeBucketRollup(time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	start := time.Unix(0, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := rollup.Add(TimedMetric{Key: strconv.Itoa(iteration % 100), At: start.Add(time.Duration(iteration) * time.Second), Value: float64(iteration)}); err != nil {
			b.Fatal(err)
		}
	}
}
