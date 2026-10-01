package hatReplication_test

import (
	"context"
	"fmt"

	"hatrie_cache/hat/hatReplication"
)

func ExampleSpaceChangefeed() {
	feed, _ := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 1,
		Capacity:      16,
	})
	subscription, _ := feed.Subscribe(feed.InitialCheckpoint())
	_, _ = feed.Publish(context.Background(), []hatReplication.SpaceChangefeedChange{{
		Key:       []byte("order-1"),
		After:     []byte(`{"status":"paid"}`),
		Operation: hatReplication.SpaceChangefeedUpdate,
	}})
	event, _ := subscription.Next(context.Background())
	_ = subscription.Ack(event.Checkpoint.Sequence)
	fmt.Printf("%d %s\n", event.Checkpoint.Sequence, event.Change.Operation)
	// Output: 1 update
}
