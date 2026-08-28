package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeBucketRollupAndVerifiedRetention(t *testing.T) {
	rollup, err := NewTimeBucketRollup(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(0, 0).UTC()
	raw := []TimedMetric{
		{Key: "u1", At: t0.Add(15 * time.Minute), Value: 2},
		{Key: "u2", At: t0.Add(30 * time.Minute), Value: 11},
		{Key: "u1", At: t0.Add(45 * time.Minute), Value: 3},
		{Key: "u1", At: t0.Add(75 * time.Minute), Value: 7},
	}
	for _, event := range raw {
		if err := rollup.Add(event); err != nil {
			t.Fatal(err)
		}
	}

	buckets, err := rollup.Buckets(t0, t0.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	want := []TimeBucket{
		{Key: "u1", Start: t0, End: t0.Add(time.Hour), Count: 2, Sum: 5, Min: 2, Max: 3},
		{Key: "u2", Start: t0, End: t0.Add(time.Hour), Count: 1, Sum: 11, Min: 11, Max: 11},
		{Key: "u1", Start: t0.Add(time.Hour), End: t0.Add(2 * time.Hour), Count: 1, Sum: 7, Min: 7, Max: 7},
	}
	if !reflect.DeepEqual(buckets, want) {
		t.Fatalf("Buckets() = %#v, want %#v", buckets, want)
	}

	cutoff := t0.Add(time.Hour)
	if err := rollup.VerifyThrough(raw, cutoff); err != nil {
		t.Fatal(err)
	}
	kept, removed, err := rollup.RetainRawAfterVerified(raw, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 || !reflect.DeepEqual(kept, raw[3:]) {
		t.Fatalf("RetainRawAfterVerified() = %#v, %d", kept, removed)
	}
}

func TestTimeBucketRollupRejectsUnverifiedRetention(t *testing.T) {
	rollup, err := NewTimeBucketRollup(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(0, 0).UTC()
	raw := []TimedMetric{{Key: "u1", At: t0.Add(10 * time.Minute), Value: 1}}
	if err := rollup.Add(raw[0]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rollup.RetainRawAfterVerified([]TimedMetric{{Key: "u1", At: raw[0].At, Value: 2}}, t0.Add(time.Hour)); err == nil {
		t.Fatal("retention accepted a rollup mismatch")
	}
	if err := rollup.VerifyThrough(raw, t0.Add(30*time.Minute)); err == nil {
		t.Fatal("verification accepted a non-boundary cutoff")
	}
	if err := rollup.Add(TimedMetric{At: t0, Value: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTimeBucketRollup(0); err == nil {
		t.Fatal("NewTimeBucketRollup accepted zero bucket")
	}
}
