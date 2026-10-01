package hatReplication

import (
	"context"
	"testing"
)

func BenchmarkTU39SpaceChangefeedPublishReadAck(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 1,
		MaxEvents:     1024,
		MaxBytes:      8 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(SpaceChangefeedSubscribeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	input := SpaceChangefeedInput{Operation: SpaceChangefeedUpdate, Key: []byte("order-1"), Before: []byte("old"), After: []byte("new")}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sequence, err := feed.Publish(input)
		if err != nil {
			b.Fatal(err)
		}
		events, err := subscription.Read(ctx, 1)
		if err != nil || len(events) != 1 {
			b.Fatalf("Read() events=%d err=%v", len(events), err)
		}
		if err := subscription.Ack(sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU39SpaceChangefeedCheckpoint(b *testing.B) {
	checkpoint, err := NewSpaceChangefeedCheckpoint("orders", 7, 42)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded[22] = byte(index)
		if _, err := UnmarshalSpaceChangefeedCheckpoint(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU39ExistingChangefeedFrontierAdvance(b *testing.B) {
	frontier := NewChangefeedFrontier(0)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := frontier.Advance(uint64(index + 1)); err != nil {
			b.Fatal(err)
		}
	}
}
