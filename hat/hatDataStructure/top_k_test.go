package hatDataStructure

import "testing"

func TestTopKTracksHeavyHittersAndBoundsMemory(t *testing.T) {
	top, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 5; index++ {
		top.Add("alpha")
	}
	for index := 0; index < 3; index++ {
		top.Add("beta")
	}
	top.Add("noise")

	entries := top.Entries()
	if len(entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(entries))
	}
	if entries[0].Key != "alpha" || entries[0].Count != 5 || entries[0].Error != 0 {
		t.Fatalf("first entry = %#v, want alpha/5/0", entries[0])
	}
	if entries[1].Key != "noise" || entries[1].Count != 4 || entries[1].Error != 3 {
		t.Fatalf("second entry = %#v, want noise/4/3", entries[1])
	}
	if top.Total() != 9 {
		t.Fatalf("total = %d, want 9", top.Total())
	}
	if entries[1].Count-entries[1].Error != 1 {
		t.Fatalf("noise lower bound = %d, want 1", entries[1].Count-entries[1].Error)
	}
}

func TestTopKMergeAndSnapshot(t *testing.T) {
	left, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewTopK[string](2)
	if err != nil {
		t.Fatal(err)
	}
	left.AddN("alpha", 5)
	left.AddN("beta", 2)
	right.AddN("alpha", 4)
	right.AddN("gamma", 3)
	if err := left.Merge(right); err != nil {
		t.Fatal(err)
	}
	entries := left.Entries()
	if entries[0].Key != "alpha" || entries[0].Count != 9 {
		t.Fatalf("merged first entry = %#v, want alpha/9", entries[0])
	}
	if entries[1].Key != "gamma" || entries[1].Count != 5 || entries[1].Error != 2 {
		t.Fatalf("merged second entry = %#v, want gamma/5/2", entries[1])
	}
	if left.Total() != 14 {
		t.Fatalf("merged total = %d, want 14", left.Total())
	}

	snapshot := left.Snapshot()
	restored, err := NewTopKFromSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got := restored.Entries()
	if len(got) != len(entries) || got[0] != entries[0] || got[1] != entries[1] {
		t.Fatalf("restored entries = %#v, want %#v", got, entries)
	}
}

func TestTopKRejectsInvalidCapacityAndSnapshot(t *testing.T) {
	if _, err := NewTopK[string](0); err == nil {
		t.Fatal("expected invalid capacity error")
	}
	if _, err := NewTopKFromSnapshot(TopKSnapshot[string]{Capacity: 1, Entries: []TopKEntry[string]{{Key: "x", Count: 0}}}); err == nil {
		t.Fatal("expected invalid snapshot error")
	}
}
