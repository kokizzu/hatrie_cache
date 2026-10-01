package hatCache

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCommandJournalSpaceFeedNextAck(b *testing.B) {
	journal, trie := openJournalSubscriptionTestFixture(b)
	feed, err := journal.SubscribeSpaceFeed(context.Background(), "orders", CommandJournalSpaceFeedOptions{
		Buffer:       256,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatalf("SubscribeSpaceFeed() error = %v", err)
	}
	defer feed.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appendJournalSubscriptionTestCommand(b, journal, trie, "orders")
		event, err := feed.Next(context.Background())
		if err != nil {
			b.Fatalf("Next() error = %v", err)
		}
		if err := feed.Ack(event.Sequence); err != nil {
			b.Fatalf("Ack() error = %v", err)
		}
	}
}

func BenchmarkCommandJournalSpaceSubscriptionNext(b *testing.B) {
	journal, trie := openJournalSubscriptionTestFixture(b)
	subscription, err := journal.SubscribeSpace(context.Background(), "orders", CommandJournalSubscribeOptions{
		Buffer:       256,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatalf("SubscribeSpace() error = %v", err)
	}
	defer subscription.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appendJournalSubscriptionTestCommand(b, journal, trie, "orders")
		select {
		case <-subscription.Records():
		case <-time.After(time.Second):
			b.Fatal("timed out waiting for journal record")
		}
	}
}
