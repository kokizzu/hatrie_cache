package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

type mu34BenchmarkCheckpointStore struct{}

func (mu34BenchmarkCheckpointStore) Load(context.Context, string) (CommandJournalSourceCheckpoint, error) {
	return CommandJournalSourceCheckpoint{}, nil
}

func (mu34BenchmarkCheckpointStore) Save(context.Context, string, CommandJournalSourceCheckpoint) error {
	return nil
}

var (
	mu34BenchmarkRecord = CommandJournalRecord{
		Sequence: 42,
		Request: CacheCommandRequest{
			Command: "SETSTR",
			Key:     "orders/1",
			Value:   "value",
		},
	}
	mu34BenchmarkRawRecord CommandJournalRecord
	mu34BenchmarkRecordOut CommandJournalRecord
)

func BenchmarkMU34RawJournalRecordNext(b *testing.B) {
	records := make(chan CommandJournalRecord, 1)
	records <- mu34BenchmarkRecord
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record := <-records
		records <- record
		mu34BenchmarkRawRecord = record
	}
}

func BenchmarkMU34HistoricalSubscriptionNext(b *testing.B) {
	records := make(chan CommandJournalRecord, 1)
	records <- mu34BenchmarkRecord
	subscription := &CommandJournalHistoricalSubscription{
		subscription: &CommandJournalSubscription{records: records},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record, ok, err := subscription.Next(ctx)
		if err != nil || !ok {
			b.Fatalf("Next() = %#v, %v, %v", record, ok, err)
		}
		records <- mu34BenchmarkRecord
		mu34BenchmarkRecordOut = record
	}
}

func BenchmarkMU34LegacyTailCheckpointCommit(b *testing.B) {
	coordinator := newMU34BenchmarkCoordinator(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := coordinator.Commit(ctx, "orders", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU34ExactCheckpointCommit(b *testing.B) {
	coordinator := newMU34BenchmarkCoordinator(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := coordinator.CommitJournalSequence(ctx, "orders", 0); err != nil {
			b.Fatal(err)
		}
	}
}

func newMU34BenchmarkCoordinator(b *testing.B) *CommandJournalSourceCheckpointCoordinator {
	b.Helper()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, mu34BenchmarkCheckpointStore{})
	if err != nil {
		b.Fatal(err)
	}
	return coordinator
}
