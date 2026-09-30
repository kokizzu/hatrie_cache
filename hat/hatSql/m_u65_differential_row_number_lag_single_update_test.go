package hatSql

import "testing"

func TestM065DifferentialSingleFrontUpdateKeepsLagState(t *testing.T) {
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: 2, MaxRows: 32})
	if err != nil {
		t.Fatalf("create window: %v", err)
	}
	seed := []DifferentialRow{
		{Key: "a", Time: 10, Diff: 1, Row: Row{"value": "a"}},
		{Key: "b", Time: 20, Diff: 1, Row: Row{"value": "b"}},
		{Key: "c", Time: 30, Diff: 1, Row: Row{"value": "c"}},
	}
	if _, err := window.Apply(seed); err != nil {
		t.Fatalf("seed window: %v", err)
	}
	if _, err := window.Apply([]DifferentialRow{{Key: "front", Time: 5, Diff: 1, Row: Row{"value": "front"}}}); err != nil {
		t.Fatalf("apply front update: %v", err)
	}

	rows, err := window.SnapshotWithError()
	if err != nil {
		t.Fatalf("snapshot window: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("row count: got %d, want 4", len(rows))
	}
	want := []struct {
		key       string
		rowNumber uint64
		hasLag    bool
		lagKey    string
	}{
		{key: "front", rowNumber: 1},
		{key: "a", rowNumber: 2},
		{key: "b", rowNumber: 3, hasLag: true, lagKey: "front"},
		{key: "c", rowNumber: 4, hasLag: true, lagKey: "a"},
	}
	for index, expected := range want {
		row := rows[index]
		if row.Key != expected.key || row.RowNumber != expected.rowNumber || row.HasLag != expected.hasLag {
			t.Fatalf("row %d: got key=%q number=%d hasLag=%t, want key=%q number=%d hasLag=%t", index, row.Key, row.RowNumber, row.HasLag, expected.key, expected.rowNumber, expected.hasLag)
		}
		if expected.hasLag && row.LagRow["value"] != expected.lagKey {
			t.Fatalf("row %d lag: got %v, want %q", index, row.LagRow["value"], expected.lagKey)
		}
	}
}

func TestM065DifferentialOutOfOrderBatchKeepsOrdering(t *testing.T) {
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: 1, MaxRows: 32})
	if err != nil {
		t.Fatalf("create window: %v", err)
	}
	updates := []DifferentialRow{
		{Key: "third", Time: 30, Diff: 1, Row: Row{"value": "third"}},
		{Key: "first", Time: 10, Diff: 1, Row: Row{"value": "first"}},
		{Key: "second", Time: 20, Diff: 1, Row: Row{"value": "second"}},
	}
	if _, err := window.Apply(updates); err != nil {
		t.Fatalf("apply batch: %v", err)
	}
	rows, err := window.SnapshotWithError()
	if err != nil {
		t.Fatalf("snapshot window: %v", err)
	}
	got := make([]string, len(rows))
	for index, row := range rows {
		got[index] = row.Key
	}
	want := []string{"first", "second", "third"}
	if len(got) != len(want) {
		t.Fatalf("row count: got %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("row %d: got %q, want %q", index, got[index], want[index])
		}
	}
}
