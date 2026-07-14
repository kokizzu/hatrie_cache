package hatriecache

import "testing"

func TestBloomFilterDataConvenienceWrappers(t *testing.T) {
	filter := newBloomFilterDataWithShape(4096, 3)
	if !filter.Add("alpha") {
		t.Fatal("Add(alpha) = false, want first insert")
	}
	if filter.Add("alpha") {
		t.Fatal("Add(alpha duplicate) = true, want false")
	}
	if added, err := filter.AddChecked(func() {}); err == nil {
		t.Fatalf("AddChecked(unsupported) = %v/nil, want error", added)
	}
	if got := filter.AddOne("beta", "gamma"); got != 2 {
		t.Fatalf("AddOne(beta, gamma) = %d, want 2", got)
	}
	if !filter.Contains("beta") || !filter.Contains("gamma") {
		t.Fatal("Contains(inserted values) = false, want true")
	}

	var nilFilter *bloomFilterData
	if nilFilter.Add("missing") {
		t.Fatal("nil Add() = true, want false")
	}
	if got := nilFilter.AddOne("missing"); got != 0 {
		t.Fatalf("nil AddOne() = %d, want 0", got)
	}
}

func TestCountMinSketchDataConvenienceWrappers(t *testing.T) {
	sketch, err := newCountMinSketchData(64, 4)
	if err != nil {
		t.Fatalf("newCountMinSketchData() error = %v", err)
	}
	if got := sketch.Add("alpha", 2); got < 2 {
		t.Fatalf("Add(alpha, 2) = %d, want at least 2", got)
	}
	if got, err := sketch.AddChecked("alpha", 3); err != nil || got < 5 {
		t.Fatalf("AddChecked(alpha, 3) = %d/%v, want at least 5/nil", got, err)
	}
	if got := sketch.AddOne("beta", 1, "gamma"); got != 1 {
		t.Fatalf("AddOne(beta, gamma) = %d, want estimate for final value 1", got)
	}
	if estimate := sketch.AddOne("gamma", 0); estimate != 1 {
		t.Fatalf("AddOne(gamma, 0) = %d, want current estimate 1", estimate)
	}
	if _, err := sketch.AddChecked(func() {}, 1); err == nil {
		t.Fatal("AddChecked(unsupported) error = nil, want error")
	}

	var nilSketch *countMinSketchData
	if got := nilSketch.Add("missing", 1); got != 0 {
		t.Fatalf("nil Add() = %d, want 0", got)
	}
	if got := nilSketch.AddOne("missing", 1); got != 0 {
		t.Fatalf("nil AddOne() = %d, want 0", got)
	}
}

func TestCuckooFilterDataConvenienceWrappers(t *testing.T) {
	filter := newCuckooFilterDataWithShape(64, maxCuckooFilterFingerprintBits)
	if !filter.Add("alpha") {
		t.Fatal("Add(alpha) = false, want first insert")
	}
	if filter.Add("alpha") {
		t.Fatal("Add(alpha duplicate) = true, want false")
	}
	if added, err := filter.AddChecked(func() {}); err == nil {
		t.Fatalf("AddChecked(unsupported) = %v/nil, want error", added)
	}
	if got := filter.AddOne("beta", "gamma"); got != 2 {
		t.Fatalf("AddOne(beta, gamma) = %d, want 2", got)
	}
	if !filter.Contains("beta") || !filter.Contains("gamma") {
		t.Fatal("Contains(inserted values) = false, want true")
	}
	if !filter.Delete("alpha") {
		t.Fatal("Delete(alpha) = false, want true")
	}
	if filter.Delete("alpha") {
		t.Fatal("Delete(alpha duplicate) = true, want false")
	}
	if got := filter.DeleteOne("beta", "gamma"); got != 2 {
		t.Fatalf("DeleteOne(beta, gamma) = %d, want 2", got)
	}
	if _, err := filter.DeleteChecked(func() {}); err == nil {
		t.Fatal("DeleteChecked(unsupported) error = nil, want error")
	}

	var nilFilter *cuckooFilterData
	if nilFilter.Add("missing") {
		t.Fatal("nil Add() = true, want false")
	}
	if got := nilFilter.AddOne("missing"); got != 0 {
		t.Fatalf("nil AddOne() = %d, want 0", got)
	}
	if nilFilter.Delete("missing") {
		t.Fatal("nil Delete() = true, want false")
	}
	if got := nilFilter.DeleteOne("missing"); got != 0 {
		t.Fatalf("nil DeleteOne() = %d, want 0", got)
	}
}
