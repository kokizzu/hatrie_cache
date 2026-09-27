package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func mz011BenchmarkFileSinkRecords(first uint64, count int) []CommandJournalRecord {
	records := make([]CommandJournalRecord, count)
	for index := range records {
		records[index] = CommandJournalRecord{Sequence: first + uint64(index)}
	}
	return records
}

func BenchmarkMZ011FileSinkCommitBatch100(b *testing.B) {
	sink, err := NewFileCommandJournalExactlyOnceSink(filepath.Join(b.TempDir(), "sink"))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		first := uint64(index*100 + 1)
		transaction, err := sink.Begin(ctx, first-1)
		if err != nil {
			b.Fatal(err)
		}
		if err := transaction.Write(ctx, mz011BenchmarkFileSinkRecords(first, 100)); err != nil {
			_ = transaction.Rollback(ctx)
			b.Fatal(err)
		}
		if err := transaction.Commit(ctx, first+99); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ011FileSinkReadBatch100(b *testing.B) {
	sink, err := NewFileCommandJournalExactlyOnceSink(filepath.Join(b.TempDir(), "sink"))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	transaction, err := sink.Begin(ctx, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := transaction.Write(ctx, mz011BenchmarkFileSinkRecords(1, 100)); err != nil {
		_ = transaction.Rollback(ctx)
		b.Fatal(err)
	}
	if err := transaction.Commit(ctx, 100); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batches, err := sink.ReadBatches(ctx)
		if err != nil || len(batches) != 1 || len(batches[0].Records) != 100 {
			b.Fatalf("ReadBatches() = %d batches, error %v", len(batches), err)
		}
	}
}
