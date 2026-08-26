package hatCache

import "testing"

func TestTopKSmallIndexPromotesAtThirdDistinctItem(t *testing.T) {
	top, err := newTopKData(8)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	if estimate, err := top.AddChecked("alpha", 1); err != nil || !estimate.Tracked {
		t.Fatalf("AddChecked(alpha) = %#v/%v, want tracked", estimate, err)
	}
	assertTopKInlineIndex(t, top, 1)
	if estimate, err := top.AddChecked("beta", 2); err != nil || !estimate.Tracked {
		t.Fatalf("AddChecked(beta) = %#v/%v, want tracked", estimate, err)
	}
	assertTopKInlineIndex(t, top, 2)

	for _, value := range []string{"alpha", "beta"} {
		if estimate := top.Estimate(value); !estimate.Tracked {
			t.Fatalf("Estimate(%q) = %#v, want tracked inline item", value, estimate)
		}
	}
	if estimate := top.Add("alpha", 3); !estimate.Tracked || estimate.Count != 4 {
		t.Fatalf("Add(alpha duplicate) = %#v, want count 4", estimate)
	}
	assertTopKInlineIndex(t, top, 2)

	smallRestored, err := newTopKDataFromSnapshot(top.Snapshot())
	if err != nil {
		t.Fatalf("newTopKDataFromSnapshot(two items) error = %v", err)
	}
	assertTopKInlineIndex(t, smallRestored, 2)

	if estimate, err := top.AddChecked("gamma", 2); err != nil || !estimate.Tracked {
		t.Fatalf("AddChecked(gamma) = %#v/%v, want tracked", estimate, err)
	}
	assertTopKMapIndex(t, top, 3)
	if estimate := top.Add("beta", 10); !estimate.Tracked || estimate.Count != 12 {
		t.Fatalf("Add(beta duplicate after promotion) = %#v, want count 12", estimate)
	}
	assertTopKMapIndex(t, top, 3)

	largeRestored, err := newTopKDataFromSnapshot(top.Snapshot())
	if err != nil {
		t.Fatalf("newTopKDataFromSnapshot(three items) error = %v", err)
	}
	assertTopKMapIndex(t, largeRestored, 3)
}

func TestTopKSmallIndexCapacityTwoEvictsWithoutMap(t *testing.T) {
	top, err := newTopKData(2)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	top.Add("alpha", 1)
	top.Add("beta", 2)
	assertTopKInlineIndex(t, top, 2)

	estimate := top.Add("gamma", 1)
	if !estimate.Tracked || estimate.Count != 2 || estimate.Error != 1 {
		t.Fatalf("Add(gamma eviction) = %#v, want count 2/error 1", estimate)
	}
	assertTopKInlineIndex(t, top, 2)
	if estimate := top.Estimate("alpha"); estimate.Tracked {
		t.Fatalf("Estimate(evicted alpha) = %#v, want untracked", estimate)
	}
	for _, value := range []string{"beta", "gamma"} {
		if estimate := top.Estimate(value); !estimate.Tracked {
			t.Fatalf("Estimate(%q) = %#v, want tracked", value, estimate)
		}
	}
}

func TestTopKSmallIndexBatchPromotesOnce(t *testing.T) {
	top, err := newTopKData(8)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	if estimate, err := top.AddOneChecked("alpha", 2, "beta", "gamma", "alpha"); err != nil || !estimate.Tracked {
		t.Fatalf("AddOneChecked(batch) = %#v/%v, want tracked", estimate, err)
	}
	assertTopKMapIndex(t, top, 3)
	if estimate := top.Estimate("alpha"); !estimate.Tracked || estimate.Count != 4 {
		t.Fatalf("Estimate(alpha after duplicate batch) = %#v, want count 4", estimate)
	}
	if estimate, err := top.AddOneChecked("beta", 0); err != nil || !estimate.Tracked || estimate.Count != 2 {
		t.Fatalf("AddOneChecked(beta, zero) = %#v/%v, want count 2", estimate, err)
	}
}

func assertTopKInlineIndex(t *testing.T, top topKData, items int) {
	t.Helper()
	if len(top.items) != items {
		t.Fatalf("top.items len = %d, want %d", len(top.items), items)
	}
	if top.byKey != nil {
		t.Fatalf("top.byKey = %#v, want nil inline index for %d items", top.byKey, items)
	}
}

func assertTopKMapIndex(t *testing.T, top topKData, items int) {
	t.Helper()
	if len(top.items) != items {
		t.Fatalf("top.items len = %d, want %d", len(top.items), items)
	}
	if len(top.byKey) != items {
		t.Fatalf("top.byKey len = %d, want %d", len(top.byKey), items)
	}
	for idx, item := range top.items {
		if got, ok := top.byKey[item.Key]; !ok || got != idx {
			t.Fatalf("top.byKey[%q] = %d/%v, want %d/true", item.Key, got, ok, idx)
		}
	}
}
