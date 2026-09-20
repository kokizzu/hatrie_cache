package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTU39SpaceChangefeedReplayCheckpointAndReconnect(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)

	setResponse := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "orders/1",
		Value:   "pending",
	})
	if !setResponse.OK {
		t.Fatalf("initial SETSTR failed: %#v", setResponse)
	}
	otherResponse := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "users/1",
		Value:   "unrelated",
	})
	if !otherResponse.OK {
		t.Fatalf("unrelated SETSTR failed: %#v", otherResponse)
	}
	deleteResponse := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "DEL",
		Key:     "orders/1",
	})
	if !deleteResponse.OK {
		t.Fatalf("DEL failed: %#v", deleteResponse)
	}

	feed, err := journal.SubscribeSpaceChangefeed(context.Background(), SpaceChangefeedOptions{
		Name:          "orders",
		SchemaVersion: 1,
		KeyPrefix:     "orders/",
		ReplayLimit:   8,
		Buffer:        2,
	})
	if err != nil {
		t.Fatalf("SubscribeSpaceChangefeed() error = %v", err)
	}
	t.Cleanup(func() { _ = feed.Close() })

	first, ok, err := feed.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("first Next() = %#v, %v, %v; want an event", first, ok, err)
	}
	if first.Sequence == 0 || first.Space != "orders" || first.SchemaVersion != 1 || first.Operation != SpaceChangefeedUpsert || first.Request.Key != "orders/1" {
		t.Fatalf("first event = %#v, want ordered orders upsert", first)
	}

	second, ok, err := feed.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("second Next() = %#v, %v, %v; want an event", second, ok, err)
	}
	if second.Sequence <= first.Sequence || second.Operation != SpaceChangefeedRetraction || !second.Retraction || second.Request.Key != "orders/1" {
		t.Fatalf("second event = %#v, want later orders retraction", second)
	}

	checkpoint := feed.Checkpoint()
	if checkpoint.Name != "orders" || checkpoint.SchemaVersion != 1 || checkpoint.Sequence != second.Sequence {
		t.Fatalf("Checkpoint() = %#v, want the second delivered sequence", checkpoint)
	}
	if err := feed.Close(); err != nil {
		t.Fatalf("feed.Close() error = %v", err)
	}

	newResponse := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "orders/2",
		Value:   "paid",
	})
	if !newResponse.OK {
		t.Fatalf("post-checkpoint SETSTR failed: %#v", newResponse)
	}

	resumed, err := journal.ResumeSpaceChangefeed(context.Background(), checkpoint, SpaceChangefeedOptions{
		Name:          "orders",
		SchemaVersion: 1,
		KeyPrefix:     "orders/",
		ReplayLimit:   8,
		Buffer:        2,
	})
	if err != nil {
		t.Fatalf("ResumeSpaceChangefeed() error = %v", err)
	}
	t.Cleanup(func() { _ = resumed.Close() })

	resumedEvent, ok, err := resumed.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("resumed Next() = %#v, %v, %v; want an event", resumedEvent, ok, err)
	}
	if resumedEvent.Sequence <= checkpoint.Sequence || resumedEvent.Request.Key != "orders/2" || resumedEvent.Operation != SpaceChangefeedUpsert {
		t.Fatalf("resumed event = %#v, want only the post-checkpoint upsert", resumedEvent)
	}

	_, err = journal.ResumeSpaceChangefeed(context.Background(), checkpoint, SpaceChangefeedOptions{
		Name:          "orders",
		SchemaVersion: 2,
		KeyPrefix:     "orders/",
	})
	if !errors.Is(err, ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("schema mismatch error = %v, want %v", err, ErrSpaceChangefeedSchemaMismatch)
	}
}

func TestTU39SpaceChangefeedValidatesNamedSchema(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)

	for name, testCase := range map[string]struct {
		options SpaceChangefeedOptions
		want    error
	}{
		"missing name": {
			options: SpaceChangefeedOptions{SchemaVersion: 1, KeyPrefix: "orders/"},
			want:    ErrSpaceChangefeedNameRequired,
		},
		"missing schema": {
			options: SpaceChangefeedOptions{Name: "orders", KeyPrefix: "orders/"},
			want:    ErrSpaceChangefeedSchemaRequired,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := journal.SubscribeSpaceChangefeed(context.Background(), testCase.options)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestTU39SpaceChangefeedPropagatesBoundedOverflow(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	feed, err := journal.SubscribeSpaceChangefeed(context.Background(), SpaceChangefeedOptions{
		Name:          "orders",
		SchemaVersion: 1,
		KeyPrefix:     "orders/",
		SkipReplay:    true,
		Buffer:        1,
		PollInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SubscribeSpaceChangefeed() error = %v", err)
	}
	t.Cleanup(func() { _ = feed.Close() })

	for index := 0; index < 8; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "orders/overflow/" + string(rune('a'+index)),
			Value:   "value",
		})
		if !response.OK {
			t.Fatalf("overflow SETSTR %d failed: %#v", index, response)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		_, ok, err := feed.Next(ctx)
		if errors.Is(err, ErrCommandJournalSubscriptionOverflow) {
			return
		}
		if err != nil {
			t.Fatalf("Next() error = %v, want bounded overflow", err)
		}
		if !ok {
			t.Fatal("Next() closed without reporting bounded overflow")
		}
	}
}
