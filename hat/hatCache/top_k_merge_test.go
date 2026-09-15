package hatCache_test

import (
	"reflect"
	"testing"

	hatCache "hatrie_cache/hat/hatCache"
)

func TestTopKMergePreservesBoundedCandidatesAndSourceIsolation(t *testing.T) {
	left := newTopKForMergeTest(t, 3, map[string]uint64{
		"alpha": 5,
		"beta":  3,
		"gamma": 1,
	})
	right := newTopKForMergeTest(t, 3, map[string]uint64{
		"alpha":   7,
		"delta":   4,
		"epsilon": 2,
	})

	if err := left.Merge(right); err != nil {
		t.Fatalf("TopK.Merge() error = %v", err)
	}
	info := left.Info()
	if info.Capacity != 3 || info.Tracked > 3 || info.Total != 22 {
		t.Fatalf("merged info = %#v, want capacity 3, at most 3 candidates, total 22", info)
	}
	items := left.Items()
	if len(items) != 3 || items[0].Value != "alpha" || items[0].Count != 12 || items[0].Error != 0 {
		t.Fatalf("merged items = %#v, want alpha first with count 12/error 0", items)
	}
	if estimate := left.Estimate("alpha"); !estimate.Tracked || estimate.Count != 12 || estimate.Error != 0 {
		t.Fatalf("merged alpha estimate = %#v, want tracked 12/0", estimate)
	}

	right.Add("delta", 100)
	if estimate := left.Estimate("delta"); !estimate.Tracked || estimate.Count != 5 {
		t.Fatalf("source mutation changed merged delta estimate = %#v", estimate)
	}
}

func TestTopKMergeAdoptsZeroValueAndRejectsCapacityMismatchAtomically(t *testing.T) {
	source := newTopKForMergeTest(t, 4, map[string]uint64{"alpha": 2, "beta": 1})
	var adopted hatCache.TopK
	if err := adopted.Merge(source); err != nil {
		t.Fatalf("zero-value Merge() error = %v", err)
	}
	if !reflect.DeepEqual(adopted.Items(), source.Items()) || adopted.Info() != source.Info() {
		t.Fatalf("zero-value adoption = %#v/%#v, want source %#v/%#v", adopted.Items(), adopted.Info(), source.Items(), source.Info())
	}

	before := adopted.Items()
	mismatch := newTopKForMergeTest(t, 3, map[string]uint64{"other": 1})
	if err := adopted.Merge(mismatch); err == nil {
		t.Fatal("capacity mismatch Merge() error = nil")
	}
	if !reflect.DeepEqual(adopted.Items(), before) {
		t.Fatalf("capacity mismatch mutated receiver = %#v, want %#v", adopted.Items(), before)
	}
}

func TestTopKAggregateStateRoundTripAndHatTrieMerge(t *testing.T) {
	source := newTopKForMergeTest(t, 3, map[string]uint64{"alpha": 4, "beta": 2, "gamma": 1})
	wire, err := source.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	restored, err := hatCache.NewTopKFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewTopKFromAggregateState() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Items(), source.Items()) || restored.Info() != source.Info() {
		t.Fatalf("state round trip = %#v/%#v, want %#v/%#v", restored.Items(), restored.Info(), source.Items(), source.Info())
	}

	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	if err := trie.MergeTopK("top", source); err != nil {
		t.Fatalf("HatTrie.MergeTopK(first) error = %v", err)
	}
	if err := trie.MergeTopK("top", source); err != nil {
		t.Fatalf("HatTrie.MergeTopK(second) error = %v", err)
	}
	items := trie.GetTopK("top")
	if len(items) != len(source.Items()) || items[0].Value != "alpha" || items[0].Count != 8 {
		t.Fatalf("HatTrie merged items = %#v, want doubled alpha state", items)
	}
}

func newTopKForMergeTest(t *testing.T, capacity uint64, values map[string]uint64) hatCache.TopK {
	t.Helper()
	top, err := hatCache.NewTopK(capacity)
	if err != nil {
		t.Fatalf("NewTopK() error = %v", err)
	}
	for value, count := range values {
		top.Add(value, count)
	}
	return top
}
