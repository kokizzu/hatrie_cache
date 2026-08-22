package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestQuantileSketchIsUsableByImporters(t *testing.T) {
	sketch, err := hatDataStructure.NewQuantileSketch(0.01)
	if err != nil {
		t.Fatalf("NewQuantileSketch() error = %v", err)
	}
	if got := sketch.Add(1, 5, 3, 2, 4); got.Count != 5 {
		t.Fatalf("Add() = %#v, want five observations", got)
	}
	if got, ok := sketch.Estimate(0.5); !ok || got.Value < 1 || got.Value > 5 || got.RankError != 1 {
		t.Fatalf("Estimate(0.5) = %#v/%v, want bounded median estimate", got, ok)
	}
}
