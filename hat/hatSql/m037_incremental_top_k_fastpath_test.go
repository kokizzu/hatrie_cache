package hatSql

import "testing"

func TestMZ037IncrementalTopKReplacementFastPath(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          2,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("create top-k: %v", err)
	}
	if _, err := topK.Apply([]DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"score": int64(10)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"score": int64(9)}},
		{Key: "c", Time: 1, Diff: 1, Row: Row{"score": int64(8)}},
	}); err != nil {
		t.Fatalf("seed top-k: %v", err)
	}

	updates := []DifferentialRow{
		{Key: "b", Diff: -1},
		{Key: "b", Time: 2, Diff: 1, Row: Row{"score": int64(11)}},
	}
	before, after, err := topK.applyReplacement(updates)
	if err != nil {
		t.Fatalf("apply replacement: %v", err)
	}
	if len(before) != 2 || before[0].node.key != "a" || before[1].node.key != "b" || before[1].node.row["score"] != int64(9) {
		t.Fatalf("before selection = %#v, want a(10), b(9)", before)
	}
	if len(after) != 2 || after[0].node.key != "b" || after[0].node.row["score"] != int64(11) || after[1].node.key != "a" {
		t.Fatalf("after selection = %#v, want b(11), a(10)", after)
	}

	snapshot := topK.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Key != "b" || snapshot[0].Row["score"] != int64(11) || snapshot[1].Key != "a" {
		t.Fatalf("snapshot after replacement = %#v, want b(11), a(10)", snapshot)
	}
}
