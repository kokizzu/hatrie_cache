package hatSql

import "testing"

func TestM064RecursiveReachabilitySingleEdgeFastPath(t *testing.T) {
	reachability := NewIncrementalRecursiveReachability()
	updates, err := reachability.appendSingleLocked(RecursiveReachabilityEdge{
		Key:  "ab",
		From: "a",
		To:   "b",
	})
	if err != nil {
		t.Fatalf("append single edge: %v", err)
	}
	want := []DifferentialRow{{
		Key:  "a\x00b",
		Diff: 1,
		Row:  Row{"from": "a", "to": "b"},
	}}
	if len(updates) != len(want) || updates[0].Key != want[0].Key || updates[0].Diff != want[0].Diff || updates[0].Row["from"] != want[0].Row["from"] || updates[0].Row["to"] != want[0].Row["to"] {
		t.Fatalf("single-edge updates = %#v, want %#v", updates, want)
	}
	if !reachability.Reachable("a", "b") {
		t.Fatal("single edge was not retained")
	}
}
