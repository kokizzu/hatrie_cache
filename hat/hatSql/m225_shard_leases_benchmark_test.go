package hatSql

import (
	"testing"
	"time"
)

var (
	m225BenchmarkLease SQLShardLease
	m225BenchmarkBytes []byte
	m225BenchmarkSink  uint64
)

func m225BenchmarkNow() time.Time {
	return time.Unix(1_700_000_000, 0).UTC()
}

func m225BenchmarkRegistry(b *testing.B, maxShards int) *SQLShardLeaseRegistry {
	b.Helper()
	registry, err := NewSQLShardLeaseRegistry(SQLShardLeaseRegistryOptions{
		ShardCount:    16,
		MaxShards:     maxShards,
		LeaseDuration: time.Minute,
		Now:           m225BenchmarkNow,
	})
	if err != nil {
		b.Fatal(err)
	}
	return registry
}

func BenchmarkM225ShardLeaseDisabledControl(b *testing.B) {
	for index := 0; index < b.N; index++ {
		m225BenchmarkSink += uint64(index)
	}
}

func BenchmarkM225ShardLeaseAcquireRelease(b *testing.B) {
	registry := m225BenchmarkRegistry(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := registry.Acquire("region-a/0", "worker-a")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := registry.Release(lease); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM225ShardLeaseGet(b *testing.B) {
	registry := m225BenchmarkRegistry(b, 1)
	lease, err := registry.Acquire("region-a/0", "worker-a")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		got, ok := registry.Get(lease.ShardID)
		if !ok {
			b.Fatal("Get() returned no lease")
		}
		m225BenchmarkLease = got
	}
}

func BenchmarkM225ShardLeaseRenew(b *testing.B) {
	registry := m225BenchmarkRegistry(b, 1)
	lease, err := registry.Acquire("region-a/0", "worker-a")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err = registry.Renew(lease)
		if err != nil {
			b.Fatal(err)
		}
		m225BenchmarkLease = lease
	}
}

func BenchmarkM225ShardLeaseMarshalBinary(b *testing.B) {
	registry := m225BenchmarkRegistry(b, 128)
	for index := 0; index < 128; index++ {
		if _, err := registry.Acquire("region-a/"+string(rune(index)), "worker-a"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := registry.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		m225BenchmarkBytes = encoded
	}
}
