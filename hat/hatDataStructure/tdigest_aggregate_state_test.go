package hatDataStructure_test

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTDigestAggregateStateRoundTripMergeAndValidation(t *testing.T) {
	left, err := hatDataStructure.NewTDigest(100)
	if err != nil {
		t.Fatal(err)
	}
	left.AddValidBatch([]float64{1, 2, 3, 4, 5})
	right, err := hatDataStructure.NewTDigest(100)
	if err != nil {
		t.Fatal(err)
	}
	right.AddValidBatch([]float64{100, 101, 102})

	wire, err := left.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	if !bytes.HasPrefix(wire, []byte("HAG1")) {
		t.Fatalf("wire prefix = %q, want HAG1", wire[:minTDigestTest(len(wire), 4)])
	}
	jsonWire, err := json.Marshal(left.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) >= len(jsonWire) {
		t.Fatalf("compact wire bytes = %d, JSON bytes = %d", len(wire), len(jsonWire))
	}

	decoded, err := hatDataStructure.NewTDigestFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewTDigestFromAggregateState() error = %v", err)
	}
	if got, want := decoded.Snapshot(), left.Snapshot(); got.Count != want.Count || len(got.Centroids) != len(want.Centroids) {
		t.Fatalf("decoded snapshot = %#v, want %#v", got, want)
	}

	rightWire, err := right.MarshalAggregateState()
	if err != nil {
		t.Fatal(err)
	}
	before := decoded.Snapshot()
	if err := decoded.MergeAggregateState(rightWire); err != nil {
		t.Fatalf("MergeAggregateState() error = %v", err)
	}
	if got := decoded.Snapshot().Count; got != before.Count+right.Snapshot().Count {
		t.Fatalf("merged count = %d, want %d", got, before.Count+right.Snapshot().Count)
	}
	median, ok := decoded.Estimate(0.5)
	if !ok || median.Value < 3 || median.Value > 100 {
		t.Fatalf("merged median = %#v, ok=%v", median, ok)
	}
	if got := right.Snapshot().Count; got != 3 {
		t.Fatalf("source count changed after merge = %d", got)
	}

	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := hatDataStructure.NewTDigestFromAggregateState(corrupt); err == nil {
		t.Fatal("corrupted aggregate state was accepted")
	}

	otherCompression, err := hatDataStructure.NewTDigest(200)
	if err != nil {
		t.Fatal(err)
	}
	otherCompression.Add(7)
	otherWire, err := otherCompression.MarshalAggregateState()
	if err != nil {
		t.Fatal(err)
	}
	unchanged := decoded.Snapshot()
	if err := decoded.MergeAggregateState(otherWire); err == nil {
		t.Fatal("compression mismatch was accepted")
	}
	if got := decoded.Snapshot(); got.Count != unchanged.Count || len(got.Centroids) != len(unchanged.Centroids) {
		t.Fatalf("digest changed after rejected merge: got=%#v want=%#v", got, unchanged)
	}

	wrongKind, err := hatDataStructure.AggregateStateEnvelope{
		Kind:    hatDataStructure.AggregateStateKindHyperLogLog,
		Version: 1,
		Payload: []byte{1},
	}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hatDataStructure.NewTDigestFromAggregateState(wrongKind); err == nil {
		t.Fatal("wrong aggregate kind was accepted")
	}
	if _, err := hatDataStructure.NewTDigestFromAggregateState(nil); err == nil {
		t.Fatal("empty aggregate state was accepted")
	}
	if math.IsNaN(median.Value) {
		t.Fatal("merged median is NaN")
	}
}

func minTDigestTest(left, right int) int {
	if left < right {
		return left
	}
	return right
}
