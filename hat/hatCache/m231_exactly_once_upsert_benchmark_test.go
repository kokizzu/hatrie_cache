package hatCache

import (
	"context"
	"fmt"
	"testing"
)

var m231BenchmarkSink uint64

type m231BenchmarkWriteTransaction struct{}

//go:noinline
func (m231BenchmarkWriteTransaction) Write(_ context.Context, records []CommandJournalRecord) error {
	m231BenchmarkSink += uint64(len(records))
	return nil
}

type m231BenchmarkUpsertTransaction struct{}

//go:noinline
func (m231BenchmarkUpsertTransaction) Upsert(context.Context, CommandJournalExactlyOnceUpsertRecord) error {
	m231BenchmarkSink++
	return nil
}

func (m231BenchmarkUpsertTransaction) Commit(context.Context, uint64) error {
	return nil
}

func (m231BenchmarkUpsertTransaction) Rollback(context.Context) error {
	return nil
}

func benchmarkM231Records(size int) []CommandJournalRecord {
	records := make([]CommandJournalRecord, size)
	for index := range records {
		records[index].Sequence = uint64(index + 1)
	}
	return records
}

func BenchmarkM231ExistingExactlyOnceBatchWrite(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("batch-%d", size), func(b *testing.B) {
			records := benchmarkM231Records(size)
			transaction := m231BenchmarkWriteTransaction{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := transaction.Write(ctx, records); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkM231ExactlyOnceUpsertBatch(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("batch-%d", size), func(b *testing.B) {
			records := benchmarkM231Records(size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				prepared, err := prepareCommandJournalExactlyOnceUpsertRecords(records, nil)
				if err != nil {
					b.Fatal(err)
				}
				transaction := m231BenchmarkUpsertTransaction{}
				for _, record := range prepared {
					if err := transaction.Upsert(ctx, record); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
