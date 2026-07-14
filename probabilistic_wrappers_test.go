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

func TestHyperLogLogDataConvenienceWrappers(t *testing.T) {
	hll, err := newHyperLogLogData(14)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	if !hll.Add("alpha") {
		t.Fatal("Add(alpha) = false, want first observation to change registers")
	}
	if hll.Add("alpha") {
		t.Fatal("Add(alpha duplicate) = true, want unchanged registers")
	}
	if changed, err := hll.AddChecked(func() {}); err == nil {
		t.Fatalf("AddChecked(unsupported) = %v/nil, want error", changed)
	}
	if changed := hll.AddOne("beta", "gamma"); changed != 2 {
		t.Fatalf("AddOne(beta, gamma) = %d, want two changed registers", changed)
	}
	if info := hll.Info(); info.Observations != 4 || info.NonZeroRegisters != 3 {
		t.Fatalf("Info() = %#v, want four observations in three registers", info)
	}

	var nilHLL *hyperLogLogData
	if nilHLL.Add("missing") {
		t.Fatal("nil Add() = true, want false")
	}
	if got := nilHLL.AddOne("missing"); got != 0 {
		t.Fatalf("nil AddOne() = %d, want 0", got)
	}
}

func TestHyperLogLogAlphaConstants(t *testing.T) {
	if got := hyperLogLogAlpha(16); got != 0.673 {
		t.Fatalf("alpha(16) = %v, want 0.673", got)
	}
	if got := hyperLogLogAlpha(32); got != 0.697 {
		t.Fatalf("alpha(32) = %v, want 0.697", got)
	}
	if got := hyperLogLogAlpha(64); got != 0.709 {
		t.Fatalf("alpha(64) = %v, want 0.709", got)
	}
	if got, want := hyperLogLogAlpha(128), 0.7213/(1+1.079/128); got != want {
		t.Fatalf("alpha(128) = %v, want %v", got, want)
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
