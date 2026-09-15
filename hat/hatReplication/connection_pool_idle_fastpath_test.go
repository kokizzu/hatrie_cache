package hatReplication

import (
	"context"
	"errors"
	"testing"
)

func TestConnectionPoolIdleAcquirePreservesReuseAndCloseContracts(t *testing.T) {
	closed := 0
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (int, error) {
			return 42, nil
		},
		Close: func(int) error {
			closed++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pool.Release(connection)

	reused, err := pool.Acquire(context.Background())
	if err != nil || reused != 42 {
		t.Fatalf("idle Acquire() = %d, %v; want 42, nil", reused, err)
	}
	pool.Release(reused)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("closed connections = %d, want 1", closed)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrConnectionPoolClosed) {
		t.Fatalf("Acquire after Close() error = %v, want ErrConnectionPoolClosed", err)
	}
}
