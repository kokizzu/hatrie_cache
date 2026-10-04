package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLPublicationSubscribeBackground(b *testing.B) {
	for i := 0; i < b.N; i++ {
		publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
		if err != nil {
			b.Fatal(err)
		}
		subscription, err := publication.Subscribe(context.Background(), SQLPublicationCheckpoint{})
		if err != nil {
			b.Fatal(err)
		}
		subscription.Close()
	}
}

func BenchmarkSQLPublicationSubscribeCancellable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		subscription, err := publication.Subscribe(ctx, SQLPublicationCheckpoint{})
		if err != nil {
			cancel()
			b.Fatal(err)
		}
		subscription.Close()
		cancel()
	}
}
