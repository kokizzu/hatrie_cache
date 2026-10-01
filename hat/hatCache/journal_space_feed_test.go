package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommandJournalSpaceFeedResumeAndCheckpoint(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	appendJournalSubscriptionTestCommand(t, journal, trie, "users")
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")

	feed, err := journal.SubscribeSpaceFeed(context.Background(), "orders", CommandJournalSpaceFeedOptions{
		ReplayLimit:  8,
		Buffer:       2,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SubscribeSpaceFeed() error = %v", err)
	}
	event, err := feed.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if event.SchemaVersion != CommandJournalSpaceFeedSchemaVersion || event.Space != "orders" || event.Sequence != 1 || event.Record.Request.Key != "orders" {
		t.Fatalf("event = %#v, want version %d orders sequence 1", event, CommandJournalSpaceFeedSchemaVersion)
	}
	if err := feed.Ack(event.Sequence); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	checkpoint := feed.Checkpoint()
	if checkpoint.SchemaVersion != CommandJournalSpaceFeedSchemaVersion || checkpoint.Space != "orders" || checkpoint.Sequence != 1 {
		t.Fatalf("checkpoint = %#v, want orders sequence 1", checkpoint)
	}
	feed.Close()

	resumed, err := journal.SubscribeSpaceFeed(context.Background(), "orders", CommandJournalSpaceFeedOptions{
		Checkpoint:   checkpoint,
		ReplayLimit:  8,
		Buffer:       2,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("resumed SubscribeSpaceFeed() error = %v", err)
	}
	defer resumed.Close()
	event, err = resumed.Next(context.Background())
	if err != nil {
		t.Fatalf("resumed Next() error = %v", err)
	}
	if event.Sequence != 3 || event.Space != "orders" {
		t.Fatalf("resumed event = %#v, want orders sequence 3", event)
	}
}

func TestCommandJournalSpaceFeedAckRequiresDeliveredSequence(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	feed, err := journal.SubscribeSpaceFeed(context.Background(), "orders", CommandJournalSpaceFeedOptions{
		Buffer:       1,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SubscribeSpaceFeed() error = %v", err)
	}
	defer feed.Close()
	if err := feed.Ack(1); !errors.Is(err, ErrCommandJournalSpaceFeedAck) {
		t.Fatalf("Ack() before delivery error = %v, want %v", err, ErrCommandJournalSpaceFeedAck)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "orders")
	event, err := feed.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if err := feed.Ack(event.Sequence); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if err := feed.Ack(event.Sequence); err != nil {
		t.Fatalf("idempotent Ack() error = %v", err)
	}
	if err := feed.Ack(event.Sequence + 1); !errors.Is(err, ErrCommandJournalSpaceFeedAck) {
		t.Fatalf("Ack() ahead error = %v, want %v", err, ErrCommandJournalSpaceFeedAck)
	}
}

func TestCommandJournalSpaceFeedRejectsInvalidCheckpointAndPolicy(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	cases := []struct {
		name       string
		checkpoint CommandJournalSpaceFeedCheckpoint
		policy     CommandJournalSpaceFeedBackpressurePolicy
		want       error
	}{
		{
			name:       "version",
			checkpoint: CommandJournalSpaceFeedCheckpoint{SchemaVersion: CommandJournalSpaceFeedSchemaVersion + 1, Space: "orders"},
			want:       ErrCommandJournalSpaceFeedCheckpoint,
		},
		{
			name:       "space",
			checkpoint: CommandJournalSpaceFeedCheckpoint{SchemaVersion: CommandJournalSpaceFeedSchemaVersion, Space: "users"},
			want:       ErrCommandJournalSpaceFeedCheckpoint,
		},
		{
			name:   "policy",
			policy: CommandJournalSpaceFeedBackpressurePolicy(99),
			want:   ErrCommandJournalSpaceFeedBackpressure,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := journal.SubscribeSpaceFeed(context.Background(), "orders", CommandJournalSpaceFeedOptions{
				Checkpoint:   tc.checkpoint,
				Backpressure: tc.policy,
				ReplayLimit:  8,
				PollInterval: time.Millisecond,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("SubscribeSpaceFeed() error = %v, want %v", err, tc.want)
			}
		})
	}
}
