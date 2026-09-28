package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM213ConsolidateQuerySubscriptionDeltasFoldsRows(t *testing.T) {
	first := Row{"id": int64(1), "payload": []byte{1, 2}}
	second := Row{"payload": []byte{9}, "id": int64(2)}
	aliases := Row{"payload": []byte{1, 2}, "id": int64(1)}

	got, err := ConsolidateQuerySubscriptionDeltas([]QuerySubscriptionDelta{
		{Row: first, Diff: 2},
		{Row: second, Diff: 1},
		{Row: aliases, Diff: -1},
	})
	if err != nil {
		t.Fatalf("ConsolidateQuerySubscriptionDeltas() error = %v", err)
	}
	want := []QuerySubscriptionDelta{
		{Row: first, Diff: 1},
		{Row: second, Diff: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("consolidated deltas = %#v, want %#v", got, want)
	}

	first["id"] = int64(99)
	first["payload"].([]byte)[0] = 8
	if got[0].Row["id"] != int64(1) || !reflect.DeepEqual(got[0].Row["payload"], []byte{1, 2}) {
		t.Fatalf("consolidated row aliases input: %#v", got[0].Row)
	}
}

func TestM213ConsolidateQuerySubscriptionDeltasRemovesZeroAndKeepsReappearanceOrder(t *testing.T) {
	rowA := Row{"id": int64(1)}
	rowB := Row{"id": int64(2)}
	rowC := Row{"id": int64(3)}

	got, err := ConsolidateQuerySubscriptionDeltas([]QuerySubscriptionDelta{
		{Row: rowA, Diff: 1},
		{Row: rowB, Diff: 1},
		{Row: rowA, Diff: -1},
		{Row: rowC, Diff: 1},
		{Row: rowA, Diff: 1},
	})
	if err != nil {
		t.Fatalf("ConsolidateQuerySubscriptionDeltas() error = %v", err)
	}
	if len(got) != 3 || got[0].Row["id"] != int64(2) || got[1].Row["id"] != int64(3) || got[2].Row["id"] != int64(1) {
		t.Fatalf("consolidated order = %#v, want b,c,a", got)
	}

	if empty, err := ConsolidateQuerySubscriptionDeltas([]QuerySubscriptionDelta{
		{Row: rowA, Diff: 1},
		{Row: rowA, Diff: -1},
	}); err != nil || empty != nil {
		t.Fatalf("zero-sum result = %#v, %v; want nil, nil", empty, err)
	}
}

func TestM213ConsolidateQuerySubscriptionDeltaBatchCopiesMetadata(t *testing.T) {
	batch := QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 8,
		Frontier: 9,
		Columns:  []string{"id", "payload"},
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(1)}, Diff: 1},
			{Row: Row{"id": int64(1)}, Diff: 1},
		},
		Progress: true,
		Complete: true,
		Reset:    true,
	}

	got, err := ConsolidateQuerySubscriptionDeltaBatch(batch)
	if err != nil {
		t.Fatalf("ConsolidateQuerySubscriptionDeltaBatch() error = %v", err)
	}
	if got.ID != batch.ID || got.Revision != batch.Revision || got.Frontier != batch.Frontier || !got.Progress || !got.Complete || !got.Reset {
		t.Fatalf("batch metadata = %#v, want preserved metadata", got)
	}
	if len(got.Columns) != 2 || len(got.Deltas) != 1 || got.Deltas[0].Diff != 2 {
		t.Fatalf("consolidated batch = %#v, want one delta and copied columns", got)
	}
	batch.Columns[0] = "changed"
	batch.Deltas[0].Row["id"] = int64(42)
	if got.Columns[0] != "id" || got.Deltas[0].Row["id"] != int64(1) {
		t.Fatalf("consolidated batch aliases input: %#v", got)
	}
}

func TestM213ConsolidateQuerySubscriptionDeltasOverflowIsAtomic(t *testing.T) {
	row := Row{"id": int64(1)}
	input := []QuerySubscriptionDelta{
		{Row: row, Diff: int64(^uint64(0) >> 1)},
		{Row: row, Diff: 1},
	}

	got, err := ConsolidateQuerySubscriptionDeltas(input)
	if !errors.Is(err, ErrQuerySubscriptionDeltaOverflow) {
		t.Fatalf("overflow error = %v, want ErrQuerySubscriptionDeltaOverflow", err)
	}
	if got != nil {
		t.Fatalf("overflow result = %#v, want nil", got)
	}
	if input[0].Diff != int64(^uint64(0)>>1) || input[1].Diff != 1 || input[0].Row["id"] != int64(1) {
		t.Fatalf("overflow mutated input: %#v", input)
	}
}

func TestM213ConsolidateQuerySubscriptionDeltasZeroAndNil(t *testing.T) {
	if got, err := ConsolidateQuerySubscriptionDeltas(nil); err != nil || got != nil {
		t.Fatalf("nil input = %#v, %v; want nil, nil", got, err)
	}
	if got, err := ConsolidateQuerySubscriptionDeltaBatch(QuerySubscriptionDeltaBatch{}); err != nil || got.Deltas != nil {
		t.Fatalf("empty batch = %#v, %v; want empty batch, nil", got, err)
	}
}
