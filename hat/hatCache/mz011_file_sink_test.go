package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mz011FileSinkRecords(first, count int) []CommandJournalRecord {
	records := make([]CommandJournalRecord, count)
	for index := range records {
		records[index] = CommandJournalRecord{Sequence: uint64(first + index)}
	}
	return records
}

func TestMZ011FileCommandJournalExactlyOnceSinkRoundTripsAndReopens(t *testing.T) {
	directory := t.TempDir()
	sink, err := NewFileCommandJournalExactlyOnceSink(directory)
	if err != nil {
		t.Fatalf("NewFileCommandJournalExactlyOnceSink() error = %v", err)
	}
	if sequence, err := sink.LoadSequence(context.Background()); err != nil || sequence != 0 {
		t.Fatalf("initial LoadSequence() = %d, %v; want 0, nil", sequence, err)
	}
	transaction, err := sink.Begin(context.Background(), 0)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	records := mz011FileSinkRecords(1, 2)
	if err := transaction.Write(context.Background(), records); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := transaction.Commit(context.Background(), 2); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if sequence, err := sink.LoadSequence(context.Background()); err != nil || sequence != 2 {
		t.Fatalf("LoadSequence() = %d, %v; want 2, nil", sequence, err)
	}
	batches, err := sink.ReadBatches(context.Background())
	if err != nil {
		t.Fatalf("ReadBatches() error = %v", err)
	}
	if len(batches) != 1 || batches[0].FirstSequence != 1 || batches[0].LastSequence != 2 || !reflect.DeepEqual(batches[0].Records, records) {
		t.Fatalf("ReadBatches() = %#v, want one batch %#v", batches, records)
	}
	reopened, err := NewFileCommandJournalExactlyOnceSink(directory)
	if err != nil {
		t.Fatalf("reopen NewFileCommandJournalExactlyOnceSink() error = %v", err)
	}
	if sequence, err := reopened.LoadSequence(context.Background()); err != nil || sequence != 2 {
		t.Fatalf("reopened LoadSequence() = %d, %v; want 2, nil", sequence, err)
	}
}

func TestMZ011FileCommandJournalExactlyOnceSinkValidatesWatermarksAndRollback(t *testing.T) {
	sink, err := NewFileCommandJournalExactlyOnceSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCommandJournalExactlyOnceSink() error = %v", err)
	}
	transaction, err := sink.Begin(context.Background(), 0)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if err := transaction.Write(context.Background(), mz011FileSinkRecords(2, 1)); !errors.Is(err, ErrFileCommandJournalSinkSequence) {
		t.Fatalf("out-of-order Write() error = %v, want ErrFileCommandJournalSinkSequence", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	transaction, err = sink.Begin(context.Background(), 0)
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if err := transaction.Write(context.Background(), mz011FileSinkRecords(1, 2)); err != nil {
		t.Fatalf("valid Write() error = %v", err)
	}
	if err := transaction.Commit(context.Background(), 1); !errors.Is(err, ErrFileCommandJournalSinkSequence) {
		t.Fatalf("wrong watermark Commit() error = %v, want ErrFileCommandJournalSinkSequence", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("second Rollback() error = %v", err)
	}
	if sequence, err := sink.LoadSequence(context.Background()); err != nil || sequence != 0 {
		t.Fatalf("LoadSequence() after rollback = %d, %v; want 0, nil", sequence, err)
	}
}

func TestMZ011FileCommandJournalExactlyOnceSinkRejectsCorruptionAndBounds(t *testing.T) {
	directory := t.TempDir()
	sink, err := NewFileCommandJournalExactlyOnceSinkWithOptions(directory, FileCommandJournalExactlyOnceSinkOptions{MaxBatchBytes: 33})
	if err != nil {
		t.Fatalf("NewFileCommandJournalExactlyOnceSinkWithOptions() error = %v", err)
	}
	transaction, err := sink.Begin(context.Background(), 0)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if err := transaction.Write(context.Background(), mz011FileSinkRecords(1, 2)); !errors.Is(err, ErrFileCommandJournalSinkBatchTooLarge) {
		t.Fatalf("oversized Write() error = %v, want ErrFileCommandJournalSinkBatchTooLarge", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if _, err := NewFileCommandJournalExactlyOnceSinkWithOptions(directory, FileCommandJournalExactlyOnceSinkOptions{MaxBatchBytes: 1}); !errors.Is(err, ErrFileCommandJournalSinkOptions) {
		t.Fatalf("invalid options error = %v, want ErrFileCommandJournalSinkOptions", err)
	}

	validSink, err := NewFileCommandJournalExactlyOnceSink(directory)
	if err != nil {
		t.Fatalf("valid sink construction error = %v", err)
	}
	transaction, err = validSink.Begin(context.Background(), 0)
	if err != nil {
		t.Fatalf("valid Begin() error = %v", err)
	}
	if err := transaction.Write(context.Background(), mz011FileSinkRecords(1, 1)); err != nil {
		t.Fatalf("valid Write() error = %v", err)
	}
	if err := transaction.Commit(context.Background(), 1); err != nil {
		t.Fatalf("valid Commit() error = %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ReadDir() = %d entries, error %v; want one committed batch", len(entries), err)
	}
	if err := os.WriteFile(filepath.Join(directory, entries[0].Name()), []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if _, err := validSink.LoadSequence(context.Background()); !errors.Is(err, ErrFileCommandJournalSinkCorrupt) {
		t.Fatalf("corrupt LoadSequence() error = %v, want ErrFileCommandJournalSinkCorrupt", err)
	}
}

func TestMZ011FileCommandJournalExactlyOnceSinkHonorsContext(t *testing.T) {
	sink, err := NewFileCommandJournalExactlyOnceSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCommandJournalExactlyOnceSink() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sink.LoadSequence(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled LoadSequence() error = %v, want context.Canceled", err)
	}
	if _, err := sink.Begin(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Begin() error = %v, want context.Canceled", err)
	}
}
