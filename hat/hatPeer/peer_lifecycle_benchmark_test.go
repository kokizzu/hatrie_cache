package hatPeer

import (
	"sync/atomic"
	"testing"
)

func BenchmarkPeerLifecycleEmitNoHooks(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPeerLifecycleEmitOneHook(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 1})
	if err != nil {
		b.Fatal(err)
	}
	var calls atomic.Int64
	if _, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) {
		calls.Add(1)
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if calls.Load() != int64(b.N) {
		b.Fatalf("hook calls = %d, want %d", calls.Load(), b.N)
	}
}

func BenchmarkPeerLifecycleSnapshot(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 32})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 32; index++ {
		if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = registry.Snapshot()
	}
}
