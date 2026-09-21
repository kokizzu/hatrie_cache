package hatDataStructure

import (
	"errors"
	"testing"
)

func TestTT007SnapshotJoinPinsWALUntilClosed(t *testing.T) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{
		MaxRecords: 2,
		MaxBytes:   4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendTT007Operation(t, journal, "op-1", 'a')

	join, err := journal.BeginSnapshotJoin(1)
	if err != nil {
		t.Fatalf("BeginSnapshotJoin() error = %v", err)
	}
	if got := join.SnapshotSequence(); got != 1 {
		t.Fatalf("SnapshotSequence() = %d, want 1", got)
	}
	appendTT007Operation(t, journal, "op-2", 'b')
	appendTT007Operation(t, journal, "op-3", 'c')

	if _, err := journal.Append(TupleFieldOperation{
		OperationID: "op-4",
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{'d'}}},
	}); !errors.Is(err, ErrTupleFieldOperationJournalLimit) {
		t.Fatalf("Append() while join is pinned = %v, want limit", err)
	}

	records, err := join.Replay(1, 0)
	if err != nil {
		t.Fatalf("join Replay() error = %v", err)
	}
	if len(records) != 2 || records[0].Sequence != 2 || records[1].Sequence != 3 {
		t.Fatalf("join Replay() = %#v, want sequences 2/3", records)
	}
	if err := join.Close(); err != nil {
		t.Fatalf("join Close() error = %v", err)
	}
	if _, err := journal.Append(TupleFieldOperation{
		OperationID: "op-4",
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{'d'}}},
	}); err != nil {
		t.Fatalf("Append() after join Close() error = %v", err)
	}
	if err := join.Close(); err != nil {
		t.Fatalf("second join Close() error = %v", err)
	}
	if _, err := join.Replay(1, 0); !errors.Is(err, ErrTupleFieldOperationJournalJoinClosed) {
		t.Fatalf("Replay() after Close() error = %v, want closed", err)
	}
}

func TestTT007SnapshotJoinRejectsUnavailableOrInvalidSequences(t *testing.T) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{
		MaxRecords: 2,
		MaxBytes:   4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendTT007Operation(t, journal, "op-1", 'a')
	appendTT007Operation(t, journal, "op-2", 'b')
	appendTT007Operation(t, journal, "op-3", 'c')

	if _, err := journal.BeginSnapshotJoin(0); !errors.Is(err, ErrTupleFieldOperationJournalGap) {
		t.Fatalf("BeginSnapshotJoin(0) error = %v, want gap", err)
	}
	if _, err := journal.BeginSnapshotJoin(4); !errors.Is(err, ErrTupleFieldOperationJournalJoinSequence) {
		t.Fatalf("BeginSnapshotJoin(4) error = %v, want sequence error", err)
	}
	join, err := journal.BeginSnapshotJoin(1)
	if err != nil {
		t.Fatal(err)
	}
	defer join.Close()
	if _, err := join.Replay(0, 0); !errors.Is(err, ErrTupleFieldOperationJournalJoinSequence) {
		t.Fatalf("Replay(0) error = %v, want sequence error", err)
	}
}

func appendTT007Operation(t *testing.T, journal *TupleFieldOperationJournal, id string, value byte) {
	t.Helper()
	if _, err := journal.Append(TupleFieldOperation{
		OperationID: id,
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{value}}},
	}); err != nil {
		t.Fatalf("Append(%q) error = %v", id, err)
	}
}
