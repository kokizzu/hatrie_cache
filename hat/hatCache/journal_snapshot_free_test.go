package hatCache

import (
	"context"
	"testing"
	"time"
)

func TestCommandJournalSubscriptionSkipReplayStartsAtCurrentTail(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "before-one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "before-two")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		SkipReplay:   true,
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	select {
	case record, ok := <-subscription.Records():
		if ok {
			t.Fatalf("snapshot-free subscription replayed record %#v", record)
		}
		t.Fatalf("snapshot-free subscription closed unexpectedly: %v", subscription.Err())
	default:
	}

	appendJournalSubscriptionTestCommand(t, journal, trie, "after")
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 3 || record.Request.Key != "after" {
		t.Fatalf("live record = %#v, want sequence 3 for after", record)
	}
}
