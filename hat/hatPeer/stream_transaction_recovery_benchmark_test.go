package hatPeer

import (
	"path/filepath"
	"testing"
)

var streamTransactionRecoveryBenchmarkSink StreamTransactionSnapshot

func BenchmarkStreamTransactionRecoveryLookup(b *testing.B) {
	recovery, err := NewStreamTransactionRecovery(StreamTransactionRecoveryOptions{MaxTransactions: 4, MaxOperations: 4})
	if err != nil {
		b.Fatal(err)
	}
	if err := recovery.Begin(1); err != nil {
		b.Fatal(err)
	}
	if err := recovery.Record(1, 1, CompactPeerStreamCall, []byte("SET"), []byte("value")); err != nil {
		b.Fatal(err)
	}

	b.Run("recovery", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			streamTransactionRecoveryBenchmarkSink, _ = recovery.Lookup(1)
		}
	})

	b.Run("direct-map-control", func(b *testing.B) {
		control := map[uint64]StreamTransactionSnapshot{
			1: {StreamID: 1, State: StreamTransactionPending},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			streamTransactionRecoveryBenchmarkSink = control[1]
		}
	})

	b.Run("recovery-into", func(b *testing.B) {
		buffered := StreamTransactionSnapshot{Operations: make([]StreamTransactionOperation, 1)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recovery.LookupInto(1, &buffered)
			streamTransactionRecoveryBenchmarkSink = buffered
		}
	})
}

func BenchmarkStreamTransactionRecoveryPersistedMutation(b *testing.B) {
	recovery, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{
		Path:            filepath.Join(b.TempDir(), "stream-recovery.bin"),
		MaxTransactions: 2,
		MaxOperations:   2,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := recovery.Begin(1); err != nil {
			b.Fatal(err)
		}
		if err := recovery.Record(1, 1, CompactPeerStreamCall, []byte("SET"), []byte("value")); err != nil {
			b.Fatal(err)
		}
		if err := recovery.Commit(1); err != nil {
			b.Fatal(err)
		}
		if err := recovery.Forget(1); err != nil {
			b.Fatal(err)
		}
	}
}
