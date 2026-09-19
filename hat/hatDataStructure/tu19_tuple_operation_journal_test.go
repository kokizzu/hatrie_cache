package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableTupleFieldOperationJournal(t *testing.T) {
	baseCount := make([]byte, 8)
	binary.BigEndian.PutUint64(baseCount, 41)
	base, err := NewPackedTuple([][]byte{[]byte("west"), baseCount, []byte("abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{
		MaxRecords: 4,
		MaxBytes:   4096,
	})
	if err != nil {
		t.Fatalf("NewTupleFieldOperationJournal() error = %v", err)
	}
	op := TupleFieldOperation{
		OperationID: "op-1",
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 1},
			{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
		},
	}
	record, err := journal.Append(op)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if record.Sequence != 1 {
		t.Fatalf("record sequence = %d, want 1", record.Sequence)
	}
	duplicate, err := journal.Append(op)
	if err != nil {
		t.Fatalf("duplicate Append() error = %v", err)
	}
	if !reflect.DeepEqual(duplicate, record) {
		t.Fatalf("duplicate record = %#v, want %#v", duplicate, record)
	}
	if got := journal.Snapshot(); len(got.Records) != 1 || got.NextSequence != 2 {
		t.Fatalf("Snapshot() = %#v, want one record and next sequence 2", got)
	}

	encoded, err := journal.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 4, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.UnmarshalBinary(encoded); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	records, err := restored.Replay(0, 0)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	updated, err := ReplayTupleFieldOperationRecords(base, records)
	if err != nil {
		t.Fatalf("ReplayTupleFieldOperationRecords() error = %v", err)
	}
	if got, _ := updated.Field(0); string(got) != "east" {
		t.Fatalf("replayed field 0 = %q, want east", got)
	}
	field, err := updated.Field(1)
	if err != nil || int64(binary.BigEndian.Uint64(field)) != 42 {
		t.Fatalf("replayed field 1 = %x, %v, want 42", field, err)
	}
	if got, _ := updated.Field(2); string(got) != "abXYZef" {
		t.Fatalf("replayed field 2 = %q, want abXYZef", got)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if err := restored.UnmarshalBinary(corrupt); !errors.Is(err, ErrTupleFieldOperationJournalChecksum) {
		t.Fatalf("corrupt UnmarshalBinary() error = %v, want checksum error", err)
	}
	if got := restored.Snapshot(); len(got.Records) != 1 || got.Records[0].Sequence != 1 {
		t.Fatalf("failed restore changed journal = %#v", got)
	}
}

func TestDurableTupleFieldOperationJournalPersistsAndRejectsConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-operations.journal")
	journal, err := OpenTupleFieldOperationJournal(path, TupleFieldOperationJournalOptions{MaxRecords: 4, MaxBytes: 4096})
	if err != nil {
		t.Fatalf("OpenTupleFieldOperationJournal() error = %v", err)
	}
	if _, err := journal.Append(TupleFieldOperation{
		OperationID: "op-1",
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("one")}},
	}); err != nil {
		t.Fatalf("persistent Append() error = %v", err)
	}
	if _, err := journal.Append(TupleFieldOperation{
		OperationID: "op-1",
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("different")}},
	}); !errors.Is(err, ErrTupleFieldOperationJournalConflict) {
		t.Fatalf("conflicting Append() error = %v, want conflict", err)
	}
	reopened, err := OpenTupleFieldOperationJournal(path, TupleFieldOperationJournalOptions{MaxRecords: 4, MaxBytes: 4096})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	if got := reopened.Snapshot(); len(got.Records) != 1 || got.Records[0].OperationID != "op-1" {
		t.Fatalf("reopened Snapshot() = %#v, want persisted op-1", got)
	}
}

func TestDurableTupleFieldOperationJournalRejectsInvalidOperationsAtomically(t *testing.T) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 2, MaxBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []TupleFieldOperation{
		{OperationID: "", Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("x")}}},
		{OperationID: "bad", Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldUpdateKind(99)}}},
	}
	for _, operation := range invalid {
		if _, err := journal.Append(operation); !errors.Is(err, ErrTupleFieldOperationJournalInvalid) {
			t.Fatalf("Append(%#v) error = %v, want invalid", operation, err)
		}
	}
	if got := journal.Snapshot(); len(got.Records) != 0 || got.NextSequence != 1 {
		t.Fatalf("invalid append changed journal = %#v", got)
	}
}

func TestDurableTupleFieldOperationJournalCompactsAndIsolatesSnapshots(t *testing.T) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if _, err := journal.Append(TupleFieldOperation{
			OperationID: "op-" + string(rune('1'+index)),
			Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{byte('a' + index)}}},
		}); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}
	snapshot := journal.Snapshot()
	if snapshot.CompactedThrough != 1 || len(snapshot.Records) != 2 || snapshot.NextSequence != 4 {
		t.Fatalf("Snapshot() = %#v, want compacted sequence 1 and records 2/3", snapshot)
	}
	if _, err := journal.Replay(0, 0); !errors.Is(err, ErrTupleFieldOperationJournalGap) {
		t.Fatalf("Replay() before compacted history error = %v, want gap", err)
	}
	records, err := journal.Replay(1, 0)
	if err != nil || len(records) != 2 || records[0].Sequence != 2 || records[1].Sequence != 3 {
		t.Fatalf("Replay(1) = %#v, %v, want sequences 2/3", records, err)
	}
	snapshot.Records[0].Updates[0].Value[0] = 'x'
	if got := journal.Snapshot().Records[0].Updates[0].Value[0]; got != 'b' {
		t.Fatalf("snapshot mutation changed journal value to %q", got)
	}
}

func TestReplayTupleFieldOperationRecordsIsAtomicOnFailure(t *testing.T) {
	cache, err := NewPackedTuple([][]byte{[]byte("before")})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := ReplayTupleFieldOperationRecords(cache, []TupleFieldOperationRecord{
		{Sequence: 1, OperationID: "ok", Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("after")}}},
		{Sequence: 2, OperationID: "bad", Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSplice, Start: 99}}},
	})
	if !errors.Is(err, ErrTupleFieldUpdateRange) {
		t.Fatalf("ReplayTupleFieldOperationRecords() error = %v, want range error", err)
	}
	if got, _ := updated.Field(0); string(got) != "before" {
		t.Fatalf("failed replay changed tuple to %q", got)
	}
}
