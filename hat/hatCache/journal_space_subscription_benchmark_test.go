package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

const (
	commandJournalSpaceSubscriptionBenchmarkRecords = 100
	commandJournalSpaceSubscriptionBenchmarkMatches = 50
)

func openCommandJournalSpaceSubscriptionBenchmarkFixture(b *testing.B) (*CommandJournal, *HatTrie) {
	b.Helper()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatalf("OpenCommandJournal() error = %v", err)
	}
	trie := CreateHatTrie()
	for index := 0; index < commandJournalSpaceSubscriptionBenchmarkRecords; index++ {
		key := "space:users"
		if index%2 == 0 {
			key = "space:orders"
		}
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     key,
			Value:   fmt.Sprintf("value-%d", index),
		})
		if !response.OK {
			_ = journal.Close()
			trie.Destroy()
			b.Fatalf("ExecuteCommand(%d) failed: %s", index, response.Message)
		}
	}
	b.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Errorf("journal.Close() error = %v", err)
		}
	})
	return journal, trie
}

func BenchmarkCommandJournalSubscriptionReplayMixed100(b *testing.B) {
	journal, _ := openCommandJournalSpaceSubscriptionBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSpaceSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSpaceSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < commandJournalSpaceSubscriptionBenchmarkRecords; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed after %d records: %v", record, subscription.Err())
			}
		}
		subscription.Close()
		if err := subscription.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommandJournalSpaceSubscriptionReplay50Of100(b *testing.B) {
	journal, _ := openCommandJournalSpaceSubscriptionBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.SubscribeSpace(context.Background(), "space:orders", CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSpaceSubscriptionBenchmarkMatches,
			Buffer:       commandJournalSpaceSubscriptionBenchmarkMatches,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < commandJournalSpaceSubscriptionBenchmarkMatches; record++ {
			entry, ok := <-subscription.Records()
			if !ok {
				b.Fatalf("space subscription closed after %d records: %v", record, subscription.Err())
			}
			if entry.Request.Key != "space:orders" {
				b.Fatalf("space subscription returned key %q", entry.Request.Key)
			}
		}
		subscription.Close()
		if err := subscription.Err(); err != nil {
			b.Fatal(err)
		}
	}
}
