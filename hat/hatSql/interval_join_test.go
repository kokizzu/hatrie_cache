package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestJoinOverlappingIntervals(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	left := []IntervalRecord{
		{Key: "u2", ID: "l3", Interval: TimeInterval{Start: t0, End: t0.Add(time.Hour)}},
		{Key: "u1", ID: "l2", Interval: TimeInterval{Start: t0.Add(2 * time.Hour), End: t0.Add(3 * time.Hour)}},
		{Key: "u1", ID: "l1", Interval: TimeInterval{Start: t0, End: t0.Add(2 * time.Hour)}},
	}
	right := []IntervalRecord{
		{Key: "u1", ID: "r2", Interval: TimeInterval{Start: t0.Add(2 * time.Hour), End: t0.Add(4 * time.Hour)}},
		{Key: "u1", ID: "r1", Interval: TimeInterval{Start: t0.Add(time.Hour), End: t0.Add(3 * time.Hour)}},
		{Key: "u2", ID: "r3", Interval: TimeInterval{Start: t0.Add(30 * time.Minute), End: t0.Add(90 * time.Minute)}},
	}

	matches, err := JoinOverlappingIntervals(left, right)
	if err != nil {
		t.Fatal(err)
	}
	pairs := make([][2]string, len(matches))
	for index, match := range matches {
		pairs[index] = [2]string{match.Left.ID, match.Right.ID}
	}
	want := [][2]string{{"l1", "r1"}, {"l2", "r1"}, {"l2", "r2"}, {"l3", "r3"}}
	if !reflect.DeepEqual(pairs, want) {
		t.Fatalf("JoinOverlappingIntervals() = %v, want %v", pairs, want)
	}
}

func TestJoinOverlappingIntervalsValidation(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	if _, err := JoinOverlappingIntervals([]IntervalRecord{{ID: "bad", Interval: TimeInterval{Start: t0, End: t0}}}, nil); err == nil {
		t.Fatal("accepted an empty interval")
	}
	if _, err := JoinOverlappingIntervals([]IntervalRecord{{Interval: TimeInterval{Start: t0, End: t0.Add(time.Hour)}}}, nil); err == nil {
		t.Fatal("accepted a record without an ID")
	}
}
