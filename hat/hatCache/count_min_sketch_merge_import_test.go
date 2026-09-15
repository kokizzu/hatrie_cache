package hatCache_test

import (
	"testing"

	"hatrie_cache/hat/hatCache"
)

func TestCountMinSketchMergeIsImportable(t *testing.T) {
	left, err := hatCache.NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatCache.NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	left.Add("alpha", 5)
	right.Add("alpha", 7)
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if got := left.Estimate("alpha"); got != 12 {
		t.Fatalf("imported merge estimate = %d, want 12", got)
	}
	restored, err := hatCache.NewCountMinSketchFromSnapshot(left.Snapshot())
	if err != nil {
		t.Fatalf("NewCountMinSketchFromSnapshot() error = %v", err)
	}
	if got := restored.Estimate("alpha"); got != 12 {
		t.Fatalf("imported restored estimate = %d, want 12", got)
	}
}
