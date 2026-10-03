package hatDataStructure

import (
	"context"
	"testing"
)

var t039BenchmarkSink uint64

func t039BenchmarkBatch(sequence uint64) SpaceChangefeedBatch {
	return SpaceChangefeedBatch{
		Sequence:      sequence,
		Frontier:      sequence,
		SchemaVersion: 7,
		Changes: []SpaceChange{{
			Operation: SpaceChangeUpdate,
			Key:       []byte("id"),
			Before:    []byte("old"),
			After:     []byte("new"),
		}},
	}
}

func BenchmarkT039SpaceChangefeed(b *testing.B) {
	b.Run("direct_channel", func(b *testing.B) {
		updates := make(chan SpaceChangefeedBatch, 1)
		batch := t039BenchmarkBatch(1)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			updates <- batch
			t039BenchmarkSink = (<-updates).Sequence
		}
	})

	b.Run("feed_append_no_subscriber", func(b *testing.B) {
		feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{
			MaxHistoryBatches: 64,
			MaxBatchChanges:   1,
		})
		if err != nil {
			b.Fatal(err)
		}
		batch := t039BenchmarkBatch(1)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			batch.Sequence = uint64(iteration + 1)
			batch.Frontier = batch.Sequence
			if err := feed.Append(batch); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("feed_append_with_subscriber", func(b *testing.B) {
		feed, err := NewSpaceChangefeed("orders", 7, SpaceChangefeedOptions{
			MaxHistoryBatches: 64,
			MaxPendingBatches: 1,
			MaxBatchChanges:   1,
		})
		if err != nil {
			b.Fatal(err)
		}
		batch := t039BenchmarkBatch(1)
		subscription, err := feed.Subscribe(context.Background(), SpaceChangefeedCheckpoint{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			batch.Sequence = uint64(iteration + 1)
			batch.Frontier = batch.Sequence
			if err := feed.Append(batch); err != nil {
				b.Fatal(err)
			}
			t039BenchmarkSink = (<-subscription.Updates()).Sequence
		}
		b.StopTimer()
		subscription.Close()
	})
}
