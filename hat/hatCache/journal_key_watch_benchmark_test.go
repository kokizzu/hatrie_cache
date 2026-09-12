package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func benchmarkJournalSubscriptionFixture(b *testing.B, matchingKey string) (*CommandJournal, *HatTrie, int) {
	b.Helper()

	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	b.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Errorf("journal.Close() error = %v", err)
		}
	})

	matchingRecords := 0
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("users/%03d", i)
		if i%4 == 0 {
			key = matchingKey
			matchingRecords++
		}
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     key,
			Value:   "value",
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand(%q) failed: %s", key, response.Message)
		}
	}
	return journal, trie, matchingRecords
}

func BenchmarkCommandJournalSubscriptionExactKeyReplayBaseline(b *testing.B) {
	const matchingKey = "orders/1"
	journal, _, matchingRecords := benchmarkJournalSubscriptionFixture(b, matchingKey)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		subscription, err := journal.SubscribeSpace(context.Background(), matchingKey, CommandJournalSubscribeOptions{
			ReplayLimit:  matchingRecords,
			Buffer:       matchingRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < matchingRecords; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed unexpectedly: %v", subscription.Err())
			}
		}
		subscription.Close()
	}
}

func BenchmarkCommandJournalSubscriptionKeyPrefixReplay(b *testing.B) {
	const matchingKey = "orders/1"
	journal, _, matchingRecords := benchmarkJournalSubscriptionFixture(b, matchingKey)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  matchingRecords,
			Buffer:       matchingRecords,
			PollInterval: time.Hour,
			KeyPrefix:    "orders/",
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < matchingRecords; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed unexpectedly: %v", subscription.Err())
			}
		}
		subscription.Close()
	}
}

func benchmarkJournalSubscriptionCoalesceFixture(b *testing.B) (*CommandJournal, *HatTrie, int) {
	b.Helper()

	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	b.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Errorf("journal.Close() error = %v", err)
		}
	})

	const matchingRecords = 128
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("users/%03d", i)
		if i < matchingRecords {
			key = fmt.Sprintf("orders/%d", i%2)
		}
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     key,
			Value:   "value",
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand(%q) failed: %s", key, response.Message)
		}
	}
	return journal, trie, matchingRecords
}

func BenchmarkCommandJournalSubscriptionPrefixReplayUncoalesced(b *testing.B) {
	journal, _, matchingRecords := benchmarkJournalSubscriptionCoalesceFixture(b)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  matchingRecords,
			Buffer:       matchingRecords,
			PollInterval: time.Hour,
			KeyPrefix:    "orders/",
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < matchingRecords; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed unexpectedly: %v", subscription.Err())
			}
		}
		subscription.Close()
	}
}

func BenchmarkCommandJournalSubscriptionPrefixReplayCoalesced(b *testing.B) {
	journal, _, matchingRecords := benchmarkJournalSubscriptionCoalesceFixture(b)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  matchingRecords,
			Buffer:       2,
			PollInterval: time.Hour,
			KeyPrefix:    "orders/",
			Coalesce:     true,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < 2; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed unexpectedly: %v", subscription.Err())
			}
		}
		subscription.Close()
	}
}
