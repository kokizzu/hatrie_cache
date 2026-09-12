package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommandJournalSubscriptionKeyPrefixFiltersReplay(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/1")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users/1")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/2")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 0,
		ReplayLimit:   10,
		Buffer:        4,
		PollInterval:  time.Hour,
		KeyPrefix:     "orders/",
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	for sequence, key := range []string{"orders/1", "orders/2"} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != uint64(sequence*2+1) {
			t.Fatalf("filtered sequence = %d, want %d", record.Sequence, sequence*2+1)
		}
		if record.Request.Key != key {
			t.Fatalf("filtered key = %q, want %q", record.Request.Key, key)
		}
	}
}

func TestCommandJournalSubscriptionCoalescesReplayByKey(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/1")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/2")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/1")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 0,
		ReplayLimit:   10,
		Buffer:        2,
		PollInterval:  time.Hour,
		KeyPrefix:     "orders/",
		Coalesce:      true,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	first := receiveJournalSubscriptionRecord(t, subscription)
	if first.Sequence != 2 || first.Request.Key != "orders/2" {
		t.Fatalf("first coalesced record = %#v, want sequence 2 for orders/2", first)
	}
	second := receiveJournalSubscriptionRecord(t, subscription)
	if second.Sequence != 3 || second.Request.Key != "orders/1" {
		t.Fatalf("second coalesced record = %#v, want sequence 3 for orders/1", second)
	}
	select {
	case record := <-subscription.Records():
		t.Fatalf("unexpected third coalesced record = %#v", record)
	case <-time.After(10 * time.Millisecond):
	}
}

func TestCommandJournalSubscriptionKeyPrefixFiltersLive(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		KeyPrefix:    "orders/",
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	journal.notifyCommandJournalSubscriptions(
		CommandJournalRecord{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "users/1"}},
		CommandJournalRecord{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "orders/1"}},
	)
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 2 || record.Request.Key != "orders/1" {
		t.Fatalf("live filtered record = %#v, want sequence 2 for orders/1", record)
	}
}

func TestCommandJournalSubscriptionKeyPrefixReplayLimitCountsMatches(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/1")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users/1")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders/2")

	_, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		ReplayLimit: 1,
		KeyPrefix:   "orders/",
	})
	if !errors.Is(err, ErrCommandJournalSubscriptionReplayLimit) {
		t.Fatalf("Subscribe() error = %v, want replay-limit error", err)
	}
}

func TestCommandJournalSubscriptionCoalescesLiveByKey(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       2,
		PollInterval: time.Hour,
		KeyPrefix:    "orders/",
		Coalesce:     true,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	for _, record := range []CommandJournalRecord{
		{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "orders/1", Value: "old"}},
		{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "orders/2", Value: "value"}},
		{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "orders/1", Value: "new"}},
	} {
		if !subscription.enqueueCoalesced(record) {
			t.Fatalf("enqueueCoalesced(%#v) unexpectedly overflowed", record)
		}
	}
	select {
	case subscription.events <- CommandJournalRecord{Sequence: 3}:
	default:
		t.Fatal("coalesced event signal was not available")
	}

	first := receiveJournalSubscriptionRecord(t, subscription)
	if first.Sequence != 2 || first.Request.Key != "orders/2" {
		t.Fatalf("first live coalesced record = %#v, want orders/2 sequence 2", first)
	}
	second := receiveJournalSubscriptionRecord(t, subscription)
	if second.Sequence != 3 || second.Request.Key != "orders/1" || second.Request.Value != "new" {
		t.Fatalf("second live coalesced record = %#v, want latest orders/1", second)
	}
}
