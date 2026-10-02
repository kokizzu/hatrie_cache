package hatPeer

import (
	"context"
	"errors"
	"testing"
)

type lifecycleBenchmarkConnection struct{}

func (lifecycleBenchmarkConnection) Close() error { return nil }

func BenchmarkConnectionPoolDoWithLifecycleHooks(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 1})
	if err != nil {
		b.Fatal(err)
	}
	for _, kind := range []PeerLifecycleKind{PeerLifecycleConnected, PeerLifecycleDisconnected} {
		if _, err := registry.Register(kind, func(PeerLifecycleEvent) {}); err != nil {
			b.Fatal(err)
		}
	}
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			return lifecycleBenchmarkConnection{}, nil
		},
		Lifecycle: registry,
		PeerID:    "benchmark",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Close(context.Background()) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Do(context.Background(), func(context.Context, Connection) error {
			return errors.New("discard")
		}); err == nil {
			b.Fatal("Do() error = nil, want discard error")
		}
	}
}
