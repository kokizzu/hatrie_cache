package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestTopKEntriesIntoReusesDestinationAndPreservesOrdering(t *testing.T) {
	top, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	top.AddN("alpha", 5)
	top.AddN("beta", 3)

	destination := make([]TopKEntry[string], 0, 2)
	entries := top.EntriesInto(destination)
	if len(entries) != 2 || entries[0].Key != "alpha" || entries[1].Key != "beta" {
		t.Fatalf("EntriesInto() = %#v, want alpha then beta", entries)
	}
	if len(entries) > 0 && &entries[0] != &destination[:1][0] {
		t.Fatal("EntriesInto() did not reuse destination backing storage")
	}

	top.AddN("beta", 4)
	entries = top.EntriesInto(entries[:0])
	want := []TopKEntry[string]{
		{Key: "beta", Count: 7, Error: 0},
		{Key: "alpha", Count: 5, Error: 0},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("reused EntriesInto() = %#v, want %#v", entries, want)
	}

	var nilTop *TopK[string]
	if got := nilTop.EntriesInto(entries); len(got) != 0 {
		t.Fatalf("nil EntriesInto() length = %d, want 0", len(got))
	}
}

func TestTopKEntriesIntoDoesNotRetainStaleEntries(t *testing.T) {
	top, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	top.Add("one")
	destination := make([]TopKEntry[string], 0, 2)
	destination = top.EntriesInto(destination)
	destination = destination[:0]
	empty, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	if got := empty.EntriesInto(destination); len(got) != 0 {
		t.Fatalf("empty EntriesInto() = %#v, want empty", got)
	}
}
