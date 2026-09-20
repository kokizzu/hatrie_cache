package hatPeer

import (
	"context"
	"errors"
	"testing"
)

type tu28BenchmarkConnection struct{}

func (tu28BenchmarkConnection) Close() error { return nil }

func BenchmarkTU28PoolDialEachCallNoLifecycle(b *testing.B) {
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			return tu28BenchmarkConnection{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		err := pool.Do(context.Background(), func(context.Context, Connection) error {
			return errors.New("force close")
		})
		if err == nil {
			b.Fatal("Do() unexpectedly succeeded")
		}
	}
}
