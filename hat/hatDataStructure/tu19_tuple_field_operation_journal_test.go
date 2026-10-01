package hatDataStructure_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var tupleFieldJournalBenchmarkSink hatDataStructure.TupleFieldOffsetCache

func TestTU19TupleFieldOperationJournalReplaysAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-operations.journal")
	initial := tu19InitialTuple(t)
	journal, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{SchemaVersion: 7})
	if err != nil {
		t.Fatalf("OpenTupleFieldOperationJournal() error = %v", err)
	}
	first, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")}})
	if err != nil {
		t.Fatalf("Apply(first) error = %v", err)
	}
	if first.Sequence != 1 || first.SchemaVersion != 7 || first.UpdateCount != 1 {
		t.Fatalf("first record = %#v", first)
	}
	if _, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 8}, {Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")}}); err != nil {
		t.Fatalf("Apply(second) error = %v", err)
	}
	if _, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{Index: 2, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1}}); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateType) {
		t.Fatalf("invalid Apply() error = %v, want type error", err)
	}
	if journal.Sequence() != 2 {
		t.Fatalf("sequence after rejected update = %d, want 2", journal.Sequence())
	}
	snapshot, err := journal.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if got, _ := snapshot.Field(0); string(got) != "east" {
		t.Fatalf("snapshot field 0 = %q", got)
	}
	if got, _ := snapshot.Field(2); string(got) != "abXYZef" {
		t.Fatalf("snapshot field 2 = %q", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{SchemaVersion: 7})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	recovered, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("recovered Snapshot() error = %v", err)
	}
	if !bytes.Equal(recovered.Bytes(), snapshot.Bytes()) || reopened.Sequence() != 2 {
		t.Fatalf("recovered tuple/sequence = %q/%d, want %q/2", recovered.Bytes(), reopened.Sequence(), snapshot.Bytes())
	}
	if got, _ := initial.Field(0); string(got) != "west" {
		t.Fatalf("initial tuple changed = %q", got)
	}
}

func TestTU19TupleFieldOperationJournalRejectsVersionAndCorruptTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-operations.journal")
	initial := tu19InitialTuple(t)
	journal, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{SchemaVersion: 3})
	if err != nil {
		t.Fatalf("open error = %v", err)
	}
	if _, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")}}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{SchemaVersion: 4}); !errors.Is(err, hatDataStructure.ErrTupleFieldJournalSchemaMismatch) {
		t.Fatalf("schema mismatch error = %v, want schema mismatch", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.Write([]byte{0x01, 0x02}); err != nil {
		file.Close()
		t.Fatalf("corrupt append error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("corrupt close error = %v", err)
	}
	if _, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{SchemaVersion: 3}); !errors.Is(err, hatDataStructure.ErrTupleFieldJournalCorrupt) {
		t.Fatalf("corrupt tail error = %v, want corrupt", err)
	}
}

func TestTU19TupleFieldOperationJournalBoundsRecordsAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuple-operations.journal")
	initial := tu19InitialTuple(t)
	journal, err := hatDataStructure.OpenTupleFieldOperationJournal(path, initial, hatDataStructure.TupleFieldOperationJournalOptions{UnsafeNoSync: true, MaxRecordBytes: 128})
	if err != nil {
		t.Fatalf("open unsafe journal error = %v", err)
	}
	defer journal.Close()
	if _, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: bytes.Repeat([]byte{'x'}, 256)}}); !errors.Is(err, hatDataStructure.ErrTupleFieldJournalRecordTooLarge) {
		t.Fatalf("oversized record error = %v, want record-too-large", err)
	}
	if journal.Sequence() != 0 {
		t.Fatalf("sequence after oversized record = %d, want 0", journal.Sequence())
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := journal.Snapshot(); !errors.Is(err, hatDataStructure.ErrTupleFieldJournalClosed) {
		t.Fatalf("Snapshot() after close error = %v, want closed", err)
	}
}

func tu19InitialTuple(t *testing.T) hatDataStructure.TupleFieldOffsetCache {
	t.Helper()
	count := make([]byte, 8)
	count[7] = 42
	cache, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("west"), count, []byte("abcdef")})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	return cache
}

func BenchmarkTU19TupleFieldOperationJournal(b *testing.B) {
	initial, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("west"), make([]byte, 8)})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("memory-only-apply", func(b *testing.B) {
		update := []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")}}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			update[0].Value[0] = byte('a' + index%26)
			updated, err := initial.ApplyUpdates(update)
			if err != nil {
				b.Fatal(err)
			}
			tupleFieldJournalBenchmarkSink = updated
		}
	})
	b.Run("unsafe-no-sync", func(b *testing.B) {
		benchmarkTU19TupleFieldOperationJournal(b, initial, true)
	})
	b.Run("durable-fsync", func(b *testing.B) {
		benchmarkTU19TupleFieldOperationJournal(b, initial, false)
	})
}

func benchmarkTU19TupleFieldOperationJournal(b *testing.B, initial hatDataStructure.TupleFieldOffsetCache, unsafeNoSync bool) {
	journal, err := hatDataStructure.OpenTupleFieldOperationJournal(filepath.Join(b.TempDir(), "tuple-operations.journal"), initial, hatDataStructure.TupleFieldOperationJournalOptions{UnsafeNoSync: unsafeNoSync})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	update := []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		update[0].Value[0] = byte('a' + index%26)
		if _, err := journal.Apply(update); err != nil {
			b.Fatal(err)
		}
	}
}
