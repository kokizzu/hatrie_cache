package hatPeer

import (
	"context"
	"testing"
)

type t237BaselineConnection struct{}

func (*t237BaselineConnection) Close() error {
	return nil
}

func t237BaselineHandler(context.Context, Connection) error {
	return nil
}

func BenchmarkT237ConnectionPoolBaseline(b *testing.B) {
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			return &t237BaselineConnection{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := pool.Close(context.Background()); err != nil {
			b.Error(err)
		}
	}()
	if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
			b.Fatal(err)
		}
	}
}
