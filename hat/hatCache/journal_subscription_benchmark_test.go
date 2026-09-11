package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

const commandJournalSubscriptionBenchmarkRecords = 100

func openCommandJournalSubscriptionBenchmarkFixture(b *testing.B, recordCount int) (*CommandJournal, *HatTrie, uint64) {
	b.Helper()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatalf("OpenCommandJournal() error = %v", err)
	}
	trie := CreateHatTrie()
	for index := 0; index < recordCount; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("benchmark:%06d", index),
			Value:   "value",
		})
		if !response.OK {
			_ = journal.Close()
			trie.Destroy()
			b.Fatalf("ExecuteCommand(%d) failed: %s", index, response.Message)
		}
	}
	tail, err := journal.Tail(0, MaxCommandJournalTailLimit)
	if err != nil {
		_ = journal.Close()
		trie.Destroy()
		b.Fatalf("Tail() error = %v", err)
	}
	b.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Errorf("journal.Close() error = %v", err)
		}
	})
	return journal, trie, tail.LastSequence
}

func BenchmarkCommandJournalTailReplay100(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		tail, err := journal.Tail(0, commandJournalSubscriptionBenchmarkRecords)
		if err != nil {
			b.Fatal(err)
		}
		if len(tail.Entries) != commandJournalSubscriptionBenchmarkRecords {
			b.Fatalf("Tail() returned %d entries, want %d", len(tail.Entries), commandJournalSubscriptionBenchmarkRecords)
		}
	}
}

func BenchmarkCommandJournalSubscriptionReplay100(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < commandJournalSubscriptionBenchmarkRecords; record++ {
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

func BenchmarkCommandJournalExecuteCommandNoSubscription(b *testing.B) {
	journal, trie, _ := openCommandJournalSubscriptionBenchmarkFixture(b, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("live:%06d", iteration),
			Value:   "value",
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
}

func BenchmarkCommandJournalSubscriptionLive(b *testing.B) {
	journal, trie, lastSequence := openCommandJournalSubscriptionBenchmarkFixture(b, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			AfterSequence: lastSequence,
			Buffer:        1,
			PollInterval:  time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("live:%06d", iteration),
			Value:   "value",
		})
		if !response.OK {
			subscription.Close()
			b.Fatal(response.Message)
		}
		select {
		case record, ok := <-subscription.Records():
			if !ok {
				b.Fatalf("subscription closed before live record: %v", subscription.Err())
			}
			if record.Sequence != lastSequence+1 {
				subscription.Close()
				b.Fatalf("live sequence = %d, want %d", record.Sequence, lastSequence+1)
			}
		case <-time.After(time.Second):
			subscription.Close()
			b.Fatal("timed out waiting for live journal record")
		}
		subscription.Close()
		if err := subscription.Err(); err != nil {
			b.Fatal(err)
		}
		lastSequence++
	}
}
