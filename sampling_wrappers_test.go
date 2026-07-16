package hatriecache

import (
	"reflect"
	"testing"
)

func TestTopKDataConvenienceWrappers(t *testing.T) {
	top, err := newTopKData(2)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	if got := top.AddOne("alpha", 2, "beta"); !got.Tracked || got.Count != 2 {
		t.Fatalf("AddOne(alpha, beta) = %#v, want beta tracked count 2", got)
	}
	if got := top.AddOne("alpha", 0); !got.Tracked || got.Count != 2 {
		t.Fatalf("AddOne(alpha, 0) = %#v, want current alpha estimate", got)
	}
	if got := top.AddOne(func() {}, 1); got != (TopKEstimate{}) {
		t.Fatalf("AddOne(unsupported) = %#v, want zero estimate", got)
	}
	if _, err := top.AddChecked(func() {}, 1); err == nil {
		t.Fatal("AddChecked(unsupported) error = nil, want error")
	}

	var nilTop *topKData
	if got := nilTop.Add("missing", 1); got != (TopKEstimate{}) {
		t.Fatalf("nil Add() = %#v, want zero estimate", got)
	}
	if got := nilTop.AddOne("missing", 1); got != (TopKEstimate{}) {
		t.Fatalf("nil AddOne() = %#v, want zero estimate", got)
	}
}

func TestReservoirSampleDataConvenienceWrappers(t *testing.T) {
	sample, err := newReservoirSampleData(2)
	if err != nil {
		t.Fatalf("newReservoirSampleData() error = %v", err)
	}
	if got := sample.Add("alpha"); !got.Accepted || got.Seen != 1 || got.Tracked != 1 {
		t.Fatalf("Add(alpha) = %#v, want first accepted item", got)
	}
	if got, err := sample.AddChecked("beta"); err != nil || !got.Accepted || got.Seen != 2 || got.Tracked != 2 {
		t.Fatalf("AddChecked(beta) = %#v/%v, want second accepted item", got, err)
	}
	if got := sample.AddOne("gamma", "delta"); got.Seen != 4 || got.Tracked != 2 {
		t.Fatalf("AddOne(gamma, delta) = %#v, want bounded sample after four seen", got)
	}
	if got := sample.Add(func() {}); got != (ReservoirSampleUpdate{}) {
		t.Fatalf("Add(unsupported) = %#v, want zero update", got)
	}
	if _, err := sample.AddChecked(func() {}); err == nil {
		t.Fatal("AddChecked(unsupported) error = nil, want error")
	}

	var nilSample *reservoirSampleData
	if got := nilSample.Add("missing"); got != (ReservoirSampleUpdate{}) {
		t.Fatalf("nil Add() = %#v, want zero update", got)
	}
	if got := nilSample.AddOne("missing"); got != (ReservoirSampleUpdate{}) {
		t.Fatalf("nil AddOne() = %#v, want zero update", got)
	}
}

func TestReservoirSamplePlainJSONStringFastAddMatchesGeneric(t *testing.T) {
	generic, err := newReservoirSampleData(3)
	if err != nil {
		t.Fatalf("newReservoirSampleData(generic) error = %v", err)
	}
	fast, err := newReservoirSampleData(3)
	if err != nil {
		t.Fatalf("newReservoirSampleData(fast) error = %v", err)
	}

	for _, value := range []string{"alpha", "beta-2", "path:/api/cache", "delta"} {
		want, err := generic.AddOneChecked(value)
		if err != nil {
			t.Fatalf("generic AddOneChecked(%q) error = %v", value, err)
		}
		got, err := fast.AddPlainJSONStringChecked(value)
		if err != nil {
			t.Fatalf("fast AddPlainJSONStringChecked(%q) error = %v", value, err)
		}
		if got != want {
			t.Fatalf("fast AddPlainJSONStringChecked(%q) = %#v, generic = %#v", value, got, want)
		}
	}

	if got, want := fast.Snapshot(), generic.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("fast reservoir sample snapshot = %#v, generic = %#v", got, want)
	}
}
