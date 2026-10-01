package hatCache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type mU34BenchmarkCheckpointStore struct {
	sequence uint64
}

func (store *mU34BenchmarkCheckpointStore) Load(context.Context) (uint64, error) {
	return store.sequence, nil
}

func (store *mU34BenchmarkCheckpointStore) Save(_ context.Context, sequence uint64) error {
	store.sequence = sequence
	return nil
}

func BenchmarkCommandJournalSubscriptionReplay100WithCheckpoint(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	store := &mU34BenchmarkCheckpointStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		store.sequence = 0
		subscription, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < commandJournalSubscriptionBenchmarkRecords; record++ {
			entry, ok := <-subscription.Records()
			if !ok {
				b.Fatalf("subscription closed after %d records: %v", record, subscription.Err())
			}
			if err := subscription.Acknowledge(context.Background(), entry.Sequence); err != nil {
				b.Fatal(err)
			}
		}
		subscription.Close()
		if err := subscription.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommandJournalSubscriptionReplay100WithFileCheckpointBatchAck(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	path := filepath.Join(b.TempDir(), "checkpoint.bin")
	store, err := NewFileCommandJournalCheckpointStore(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			b.Fatal(err)
		}
		b.StartTimer()
		subscription, err := journal.SubscribeWithCheckpoint(context.Background(), store, CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		var lastSequence uint64
		for record := 0; record < commandJournalSubscriptionBenchmarkRecords; record++ {
			entry, ok := <-subscription.Records()
			if !ok {
				b.Fatalf("subscription closed after %d records: %v", record, subscription.Err())
			}
			lastSequence = entry.Sequence
		}
		if err := subscription.Acknowledge(context.Background(), lastSequence); err != nil {
			b.Fatal(err)
		}
		subscription.Close()
		if err := subscription.Err(); err != nil {
			b.Fatal(err)
		}
	}
}
