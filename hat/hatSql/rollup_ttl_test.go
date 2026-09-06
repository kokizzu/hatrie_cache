package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeBucketRollupExpiresOnlyCompleteBuckets(t *testing.T) {
	rollup, err := NewTimeBucketRollup(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(0, 0).UTC()
	for _, event := range []TimedMetric{
		{Key: "user", At: t0.Add(5 * time.Minute), Value: 1},
		{Key: "user", At: t0.Add(65 * time.Minute), Value: 2},
		{Key: "user", At: t0.Add(125 * time.Minute), Value: 3},
	} {
		if err := rollup.Add(event); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := rollup.ExpireBefore(t0.Add(2 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("ExpireBefore() removed %d buckets, want 2", removed)
	}
	got, err := rollup.Buckets(t0, t0.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	want := []TimeBucket{{Key: "user", Start: t0.Add(2 * time.Hour), End: t0.Add(3 * time.Hour), Count: 1, Sum: 3, Min: 3, Max: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("remaining buckets = %#v, want %#v", got, want)
	}
}

func TestTimeBucketRollupExpireBeforeRejectsNonBoundaryWithoutMutation(t *testing.T) {
	rollup, err := NewTimeBucketRollup(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(0, 0).UTC()
	if err := rollup.Add(TimedMetric{Key: "user", At: t0.Add(5 * time.Minute), Value: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := rollup.ExpireBefore(t0.Add(30 * time.Minute)); err == nil {
		t.Fatal("ExpireBefore() accepted a non-boundary cutoff")
	}
	got, err := rollup.Buckets(t0, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("bucket count after rejected prune = %d, want 1", len(got))
	}
}

func TestTimeBucketRollupExpireBeforeHandlesEmptyAndNil(t *testing.T) {
	rollup, err := NewTimeBucketRollup(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := rollup.ExpireBefore(time.Unix(0, 0).UTC()); err != nil || removed != 0 {
		t.Fatalf("empty ExpireBefore() = %d/%v, want 0/nil", removed, err)
	}
	var nilRollup *TimeBucketRollup
	if _, err := nilRollup.ExpireBefore(time.Unix(0, 0).UTC()); err == nil {
		t.Fatal("nil rollup ExpireBefore() error = nil")
	}
}

func BenchmarkTimeBucketRollupExpireBefore(b *testing.B) {
	const bucketCount = 1024
	start := time.Unix(0, 0).UTC()
	cutoff := start.Add(512 * time.Hour)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		rollup, err := NewTimeBucketRollup(time.Hour)
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < bucketCount; index++ {
			bucketStart := start.Add(time.Duration(index) * time.Hour)
			rollup.buckets[timeBucketKey{key: "metric", start: bucketStart.UnixNano()}] = TimeBucket{
				Key: "metric", Start: bucketStart, End: bucketStart.Add(time.Hour), Count: 1, Sum: 1, Min: 1, Max: 1,
			}
		}
		b.StartTimer()
		removed, err := rollup.ExpireBefore(cutoff)
		b.StopTimer()
		if err != nil || removed != 512 {
			b.Fatalf("ExpireBefore() = %d/%v, want 512/nil", removed, err)
		}
	}
}
