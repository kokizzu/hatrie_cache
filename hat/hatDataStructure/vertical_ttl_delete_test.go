package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPersistentDeleteBitmapAppliesVerticalTTLDeletes(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(6)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap() error = %v", err)
	}
	if _, err := bitmap.Delete(1); err != nil {
		t.Fatalf("Delete(existing) error = %v", err)
	}
	keys := []string{"a", "b", "c", "d", "e", "f"}
	now := time.Unix(100, 0)
	expires := []int64{
		now.Add(time.Hour).UnixNano(),
		now.Add(-time.Hour).UnixNano(),
		now.Add(-time.Minute).UnixNano(),
		0,
		now.UnixNano(),
		now.Add(time.Hour).UnixNano(),
	}
	result, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, now, 0)
	if err != nil {
		t.Fatalf("ApplyVerticalTTLDeletes() error = %v", err)
	}
	want := []VerticalTTLDeleteCandidate{
		{Row: 2, Key: "c"},
		{Row: 4, Key: "e"},
	}
	if !reflect.DeepEqual(result.Candidates, want) {
		t.Fatalf("Candidates = %#v, want %#v", result.Candidates, want)
	}
	if result.Deleted != 2 {
		t.Fatalf("Deleted = %d, want 2", result.Deleted)
	}
	if !bitmap.Contains(1) || !bitmap.Contains(2) || !bitmap.Contains(4) {
		t.Fatalf("bitmap did not retain all deleted rows")
	}
	if bitmap.Deleted() != 3 {
		t.Fatalf("bitmap.Deleted() = %d, want 3", bitmap.Deleted())
	}
	again, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, now, 0)
	if err != nil {
		t.Fatalf("second ApplyVerticalTTLDeletes() error = %v", err)
	}
	if len(again.Candidates) != 0 || again.Deleted != 0 {
		t.Fatalf("second result = %#v, want no new candidates", again)
	}
}

func TestPersistentDeleteBitmapVerticalTTLDeleteBoundsAndValidation(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(3)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap() error = %v", err)
	}
	now := time.Unix(200, 0)
	keys := []string{"a", "b", "c"}
	expires := []int64{now.UnixNano(), now.UnixNano(), now.UnixNano()}
	limited, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, now, 1)
	if err != nil {
		t.Fatalf("limited ApplyVerticalTTLDeletes() error = %v", err)
	}
	if len(limited.Candidates) != 1 || limited.Candidates[0].Row != 0 {
		t.Fatalf("limited candidates = %#v", limited.Candidates)
	}
	if _, err := bitmap.ApplyVerticalTTLDeletes(keys[:2], expires, now, 0); !errors.Is(err, ErrVerticalTTLDeleteInvalid) {
		t.Fatalf("length mismatch error = %v", err)
	}
	if _, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, now, -1); !errors.Is(err, ErrVerticalTTLDeleteInvalid) {
		t.Fatalf("negative limit error = %v", err)
	}
	if _, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, time.Time{}, 0); !errors.Is(err, ErrVerticalTTLDeleteInvalid) {
		t.Fatalf("zero time error = %v", err)
	}
}

func TestPersistentDeleteBitmapAppliesVerticalTTLDeletesIntoBuffer(t *testing.T) {
	bitmap, err := NewPersistentDeleteBitmap(4)
	if err != nil {
		t.Fatalf("NewPersistentDeleteBitmap() error = %v", err)
	}
	now := time.Unix(300, 0)
	keys := []string{"a", "b", "c", "d"}
	expires := []int64{now.UnixNano(), 0, now.UnixNano(), now.UnixNano()}
	buffer := make([]VerticalTTLDeleteCandidate, 0, 3)
	result, err := bitmap.ApplyVerticalTTLDeletesInto(buffer, keys, expires, now, 0)
	if err != nil {
		t.Fatalf("ApplyVerticalTTLDeletesInto() error = %v", err)
	}
	want := []VerticalTTLDeleteCandidate{{Row: 0, Key: "a"}, {Row: 2, Key: "c"}, {Row: 3, Key: "d"}}
	if !reflect.DeepEqual(result.Candidates, want) {
		t.Fatalf("Candidates = %#v, want %#v", result.Candidates, want)
	}
	if len(result.Candidates) != len(buffer) || &result.Candidates[0] != &buffer[0] {
		t.Fatalf("result did not reuse caller buffer")
	}
	for _, candidate := range result.Candidates {
		if _, err := bitmap.Undelete(candidate.Row); err != nil {
			t.Fatalf("Undelete(%d) error = %v", candidate.Row, err)
		}
	}
	result, err = bitmap.ApplyVerticalTTLDeletesInto(result.Candidates[:0], keys, expires, now, 2)
	if err != nil {
		t.Fatalf("limited ApplyVerticalTTLDeletesInto() error = %v", err)
	}
	if len(result.Candidates) != 2 || result.Deleted != 2 {
		t.Fatalf("limited result = %#v, want two candidates and deletes", result)
	}
}
