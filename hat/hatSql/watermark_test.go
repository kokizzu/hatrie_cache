package hatSql_test

import (
	"errors"
	"testing"
	"time"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestMergeWatermarksUsesSlowestSource(t *testing.T) {
	first := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	watermarks := []time.Time{
		first.Add(5 * time.Minute),
		first.Add(2 * time.Minute),
		first.Add(9 * time.Minute),
	}

	got, err := hatSql.MergeWatermarks(watermarks)
	if err != nil {
		t.Fatalf("MergeWatermarks() error = %v", err)
	}
	want := first.Add(2 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("MergeWatermarks() = %v, want %v", got, want)
	}

	watermarks[0], watermarks[2] = watermarks[2], watermarks[0]
	reordered, err := hatSql.MergeWatermarks(watermarks)
	if err != nil {
		t.Fatalf("MergeWatermarks() reordered error = %v", err)
	}
	if !reordered.Equal(want) {
		t.Fatalf("MergeWatermarks() reordered = %v, want %v", reordered, want)
	}
}

func TestAdvanceWatermarkNeverMovesBackward(t *testing.T) {
	previous := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	if got := hatSql.AdvanceWatermark(previous, previous.Add(-time.Minute)); !got.Equal(previous) {
		t.Fatalf("AdvanceWatermark(backward) = %v, want %v", got, previous)
	}
	forward := previous.Add(time.Minute)
	if got := hatSql.AdvanceWatermark(previous, forward); !got.Equal(forward) {
		t.Fatalf("AdvanceWatermark(forward) = %v, want %v", got, forward)
	}
}

func TestMergeWatermarksRejectsEmptyInput(t *testing.T) {
	if _, err := hatSql.MergeWatermarks(nil); !errors.Is(err, hatSql.ErrWatermarkInvalid) {
		t.Fatalf("MergeWatermarks(nil) error = %v, want ErrWatermarkInvalid", err)
	}
}

func BenchmarkMergeWatermarks(b *testing.B) {
	base := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	watermarks := []time.Time{base.Add(time.Minute), base, base.Add(2 * time.Minute)}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.MergeWatermarks(watermarks); err != nil {
			b.Fatal(err)
		}
	}
}
