package hatDataStructure

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTupleFieldUpdateJournalReopensAndReplaysIntoVersionedTuple(t *testing.T) {
	format, err := NewTupleFormat(7, []TupleFieldSpec{
		{Name: "name", Type: TupleFieldString},
		{Name: "count", Type: TupleFieldInt64},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	initial, err := format.PackVersioned([]TupleFieldValue{TupleString("alpha"), TupleInt64(40)})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "tuple-updates.journal")
	journal, err := OpenTupleFieldUpdateJournal(path)
	if err != nil {
		t.Fatalf("OpenTupleFieldUpdateJournal() error = %v", err)
	}
	if sequence, err := journal.Append(format.Version(), []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("beta")}}); err != nil || sequence != 1 {
		t.Fatalf("Append(set) = %d/%v, want 1/nil", sequence, err)
	}
	if sequence, err := journal.Append(format.Version(), []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSplice, Start: 1, Remove: 1, Insert: []byte("R")},
		{Index: 1, Kind: TupleFieldAddInt64, Delta: 2},
	}); err != nil || sequence != 2 {
		t.Fatalf("Append(splice/add) = %d/%v, want 2/nil", sequence, err)
	}
	if got := journal.LastSequence(); got != 2 {
		t.Fatalf("LastSequence() = %d, want 2", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	journal, err = OpenTupleFieldUpdateJournal(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer journal.Close()
	updated, through, err := journal.ReplayInto(format, initial, 0)
	if err != nil {
		t.Fatalf("ReplayInto() error = %v", err)
	}
	if through != 2 {
		t.Fatalf("ReplayInto() sequence = %d, want 2", through)
	}
	values, err := format.Unpack(updated.Tuple())
	if err != nil {
		t.Fatalf("Unpack() error = %v", err)
	}
	if values[0].String != "bRta" || values[1].Int64 != 42 {
		t.Fatalf("replayed values = %#v, want bRta/42", values)
	}

	seen := 0
	through, err = journal.Replay(1, func(record TupleFieldUpdateJournalRecord) error {
		seen++
		if record.Sequence != 2 || record.SchemaVersion != format.Version() || len(record.Updates) != 2 {
			t.Fatalf("replayed record = %#v, want sequence 2/schema 7/two updates", record)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if seen != 1 || through != 2 {
		t.Fatalf("Replay(after 1) = %d records through %d, want 1/2", seen, through)
	}
}

func TestTupleFieldUpdateJournalTruncatesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-updates.journal")
	journal, err := OpenTupleFieldUpdateJournal(path)
	if err != nil {
		t.Fatalf("OpenTupleFieldUpdateJournal() error = %v", err)
	}
	if _, err := journal.Append(1, []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("stable")}}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("Write(torn tail) error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close(torn tail) error = %v", err)
	}

	reopened, err := OpenTupleFieldUpdateJournal(path)
	if err != nil {
		t.Fatalf("reopen after torn tail error = %v", err)
	}
	defer reopened.Close()
	if got := reopened.LastSequence(); got != 1 {
		t.Fatalf("LastSequence() after torn tail = %d, want 1", got)
	}
}

func TestTupleFieldUpdateJournalAppendBatchAssignsSequencesAndValidatesBeforeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-updates.journal")
	journal, err := OpenTupleFieldUpdateJournalWithOptions(path, TupleFieldUpdateJournalOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenTupleFieldUpdateJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	sequences, err := journal.AppendBatch([]TupleFieldUpdateJournalAppend{
		{SchemaVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("one")}}},
		{SchemaVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("two")}}},
	})
	if err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("AppendBatch() sequences = %#v, want [1 2]", sequences)
	}
	if err := journal.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	pathBeforeRejectedBatch := journal.LastSequence()
	if _, err := journal.AppendBatch([]TupleFieldUpdateJournalAppend{
		{SchemaVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("three")}}},
		{SchemaVersion: 0, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("rejected")}}},
	}); !errors.Is(err, ErrTupleUpdateJournalSchemaVersion) {
		t.Fatalf("AppendBatch(rejected) error = %v, want schema-version error", err)
	}
	if got := journal.LastSequence(); got != pathBeforeRejectedBatch {
		t.Fatalf("LastSequence() after rejected batch = %d, want %d", got, pathBeforeRejectedBatch)
	}
}

func TestTupleFieldUpdateJournalRejectsInvalidRecords(t *testing.T) {
	journal, err := OpenTupleFieldUpdateJournal(filepath.Join(t.TempDir(), "tuple-updates.journal"))
	if err != nil {
		t.Fatalf("OpenTupleFieldUpdateJournal() error = %v", err)
	}
	defer journal.Close()

	tests := []struct {
		name    string
		schema  uint64
		updates []TupleFieldUpdate
		wantErr error
	}{
		{name: "schema version", updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("x")}}, wantErr: ErrTupleUpdateJournalSchemaVersion},
		{name: "field index", schema: 1, updates: []TupleFieldUpdate{{Index: -1, Kind: TupleFieldSet, Value: []byte("x")}}, wantErr: ErrTupleUpdateJournalUpdateInvalid},
		{name: "empty batch", schema: 1, wantErr: ErrTupleUpdateJournalUpdateInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := journal.Append(test.schema, test.updates)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Append() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	if got := journal.LastSequence(); got != 0 {
		t.Fatalf("LastSequence() after rejected appends = %d, want 0", got)
	}
}

func TestTupleFieldUpdateJournalRejectsChecksumCorruption(t *testing.T) {
	encoded, err := MarshalTupleFieldUpdateJournalRecord(TupleFieldUpdateJournalRecord{
		Sequence:      1,
		SchemaVersion: 1,
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("value")}},
	})
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	encoded[len(encoded)-1] ^= 1
	if _, err := UnmarshalTupleFieldUpdateJournalRecord(encoded); !errors.Is(err, ErrTupleUpdateJournalCorrupt) {
		t.Fatalf("UnmarshalTupleFieldUpdateJournalRecord() error = %v, want corruption", err)
	}
}
