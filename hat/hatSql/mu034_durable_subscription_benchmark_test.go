package hatSql

import (
	"context"
	"testing"
)

type mu034BenchmarkCheckpointStore struct {
	checkpoint SQLPublicationCheckpoint
}

func (store *mu034BenchmarkCheckpointStore) Load(context.Context, string, string) (SQLPublicationCheckpoint, bool, error) {
	return store.checkpoint, store.checkpoint.Revision != 0, nil
}

func (store *mu034BenchmarkCheckpointStore) Save(_ context.Context, _, _ string, checkpoint SQLPublicationCheckpoint) error {
	store.checkpoint = checkpoint
	return nil
}

func BenchmarkSQLPublicationSubscriptionBaseline(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{MaxHistoryBatches: 8})
		if err != nil {
			b.Fatal(err)
		}
		if err := publication.Append(SQLPublicationBatch{
			Revision: 1,
			Frontier: 1,
			Deltas:   []SQLPublicationDelta{{Row: Row{"id": uint64(i)}, Diff: 1}},
		}); err != nil {
			b.Fatal(err)
		}
		subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
		if err != nil {
			b.Fatal(err)
		}
		<-subscription.Updates()
		if err := subscription.Ack(SQLPublicationCheckpoint{Revision: 1, Frontier: 1}); err != nil {
			b.Fatal(err)
		}
		subscription.Close()
	}
}

func BenchmarkSQLPublicationDurableSubscription(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{MaxHistoryBatches: 8})
		if err != nil {
			b.Fatal(err)
		}
		if err := publication.Append(SQLPublicationBatch{
			Revision: 1,
			Frontier: 1,
			Deltas:   []SQLPublicationDelta{{Row: Row{"id": uint64(i)}, Diff: 1}},
		}); err != nil {
			b.Fatal(err)
		}
		store := &mu034BenchmarkCheckpointStore{}
		subscription, err := publication.SubscribeDurable(context.Background(), "benchmark", store)
		if err != nil {
			b.Fatal(err)
		}
		<-subscription.Updates()
		if err := subscription.AckWithStore(context.Background(), store, SQLPublicationCheckpoint{Revision: 1, Frontier: 1}); err != nil {
			b.Fatal(err)
		}
		if err := subscription.Cancel(context.Background(), store); err != nil {
			b.Fatal(err)
		}
	}
}
