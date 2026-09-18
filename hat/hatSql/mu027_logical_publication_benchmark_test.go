package hatSql

import (
	"context"
	"testing"
)

var mu027BenchmarkSink uint64

func mu027BenchmarkBatch(revision uint64) SQLPublicationBatch {
	return SQLPublicationBatch{
		Revision: revision,
		Frontier: revision,
		Deltas: []SQLPublicationDelta{
			{Row: Row{"id": int64(revision), "value": "ready"}, Diff: 1},
		},
	}
}

func BenchmarkMU027BeforeDirectBatchSend(b *testing.B) {
	updates := make(chan SQLPublicationBatch, 1)
	batch := mu027BenchmarkBatch(1)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		updates <- batch
		mu027BenchmarkSink = (<-updates).Revision
	}
}

func BenchmarkMU027AfterPublicationAppend(b *testing.B) {
	publication, err := NewSQLPublication("orders", []string{"id", "value"}, SQLPublicationOptions{
		MaxHistoryBatches: 64,
		MaxBatchDeltas:    1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := publication.Append(mu027BenchmarkBatch(uint64(iteration + 1))); err != nil {
			b.Fatal(err)
		}
	}
	mu027BenchmarkSink = publication.Snapshot().LatestRevision
}

func BenchmarkMU027AfterPublicationAppendWithSubscriber(b *testing.B) {
	publication, err := NewSQLPublication("orders", []string{"id", "value"}, SQLPublicationOptions{
		MaxHistoryBatches: 64,
		MaxBatchDeltas:    1,
		MaxPendingBatches: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		revision := uint64(iteration + 1)
		if err := publication.Append(mu027BenchmarkBatch(revision)); err != nil {
			b.Fatal(err)
		}
		mu027BenchmarkSink = (<-subscription.Updates()).Revision
	}
	b.StopTimer()
	subscription.Close()
}

func BenchmarkMU027AfterPublicationReplayAndAck(b *testing.B) {
	publication, err := NewSQLPublication("orders", []string{"id", "value"}, SQLPublicationOptions{
		MaxHistoryBatches: 64,
		MaxBatchDeltas:    1,
		MaxPendingBatches: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	for revision := uint64(1); revision <= 64; revision++ {
		if err := publication.Append(mu027BenchmarkBatch(revision)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
		if err != nil {
			b.Fatal(err)
		}
		var checkpoint SQLPublicationCheckpoint
		for range 64 {
			batch := <-subscription.Updates()
			checkpoint = SQLPublicationCheckpoint{Revision: batch.Revision, Frontier: batch.Frontier}
		}
		if err := subscription.Ack(checkpoint); err != nil {
			b.Fatal(err)
		}
		mu027BenchmarkSink = checkpoint.Revision
		subscription.Close()
	}
}
