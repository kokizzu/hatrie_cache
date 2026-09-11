package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommandJournalSpaceSubscriptionReplaysOnlyMatchingSpace(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	appendJournalSubscriptionTestCommand(t, journal, trie, "products")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users")

	subscription, err := journal.SubscribeSpace(context.Background(), "orders", CommandJournalSubscribeOptions{
		AfterSequence: 1,
		ReplayLimit:   2,
		Buffer:        2,
		PollInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SubscribeSpace() error = %v", err)
	}
	defer subscription.Close()

	for _, wantSequence := range []uint64{2, 4} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != wantSequence || record.Request.Key != "orders" {
			t.Fatalf("space replay record = %#v, want orders sequence %d", record, wantSequence)
		}
	}

	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 7 || record.Request.Key != "orders" {
		t.Fatalf("space live record = %#v, want orders sequence 7", record)
	}
}

func TestCommandJournalSpaceSubscriptionAdvancesPastUnrelatedLiveRecords(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.SubscribeSpace(context.Background(), "orders", CommandJournalSubscribeOptions{
		Buffer:       2,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("SubscribeSpace() error = %v", err)
	}
	defer subscription.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	for _, wantSequence := range []uint64{2, 4} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != wantSequence || record.Request.Key != "orders" {
			t.Fatalf("space live record = %#v, want orders sequence %d", record, wantSequence)
		}
	}

	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 5 || record.Request.Key != "orders" {
		t.Fatalf("space post-gap record = %#v, want orders sequence 5", record)
	}
}

func TestCommandJournalSpaceSubscriptionRequiresSpace(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	if _, err := journal.SubscribeSpace(context.Background(), "", CommandJournalSubscribeOptions{}); !errors.Is(err, ErrCommandJournalSubscriptionSpaceRequired) {
		t.Fatal("SubscribeSpace() accepted an empty space")
	}
}

func TestCommandJournalSpaceSubscriptionCountsOnlyMatchingReplayRecords(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")

	_, err := journal.SubscribeSpace(context.Background(), "orders", CommandJournalSubscribeOptions{
		ReplayLimit: 1,
	})
	if !errors.Is(err, ErrCommandJournalSubscriptionReplayLimit) {
		t.Fatalf("SubscribeSpace() error = %v, want %v", err, ErrCommandJournalSubscriptionReplayLimit)
	}
}

func TestCommandJournalSpaceSubscriptionDeliversBufferedAppendAfterJournalClose(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.SubscribeSpace(context.Background(), "orders", CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("SubscribeSpace() error = %v", err)
	}
	defer subscription.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 2 || record.Request.Key != "orders" {
		t.Fatalf("space buffered record = %#v, want orders sequence 2", record)
	}
}
