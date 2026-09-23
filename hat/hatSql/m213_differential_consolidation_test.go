package hatSql

import (
	"errors"
	"testing"
)

func TestM213DifferentialBatchConsolidatesEqualRowsBeforeDownstreamApply(t *testing.T) {
	batch := QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(1), "value": "Ada"}, Diff: 1},
			{Row: Row{"id": int64(1), "value": "Ada"}, Diff: -1},
			{Row: Row{"id": int64(2), "value": "Lin"}, Diff: 2},
		},
	}
	consolidated, err := batch.Consolidate()
	if err != nil {
		t.Fatalf("Consolidate() error = %v", err)
	}
	if len(consolidated.Deltas) != 1 || consolidated.Deltas[0].Diff != 2 || consolidated.Deltas[0].Row["id"] != int64(2) {
		t.Fatalf("consolidated batch = %#v, want only id 2 with diff 2", consolidated.Deltas)
	}
	if !consolidated.consolidated {
		t.Fatal("consolidated batch is not marked as normalized")
	}
	consolidated.Deltas[0].Row["value"] = "mutated"
	if batch.Deltas[2].Row["value"] != "Lin" {
		t.Fatal("Consolidate() returned an aliased row map")
	}
}

func TestM213DifferentialBatchConsolidationRejectsOverflow(t *testing.T) {
	batch := QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(1)}, Diff: int64(^uint64(0) >> 1)},
			{Row: Row{"id": int64(1)}, Diff: 1},
		},
	}
	_, err := batch.Consolidate()
	if !errors.Is(err, ErrQuerySubscriptionDeltaOverflow) {
		t.Fatalf("Consolidate() error = %v, want overflow", err)
	}
}

func TestM213DifferentialPublisherMarksGeneratedBatchesConsolidated(t *testing.T) {
	batch := querySubscriptionInitialDeltaWithOrder(QuerySubscriptionSnapshot{
		Result: QueryResult{Rows: []Row{{"id": int64(1)}, {"id": int64(1)}}},
	}, false)
	if !batch.consolidated {
		t.Fatal("generated differential batch is not marked consolidated")
	}
	if len(batch.Deltas) != 1 || batch.Deltas[0].Diff != 2 {
		t.Fatalf("generated batch = %#v, want one multiplicity-two delta", batch.Deltas)
	}
}
