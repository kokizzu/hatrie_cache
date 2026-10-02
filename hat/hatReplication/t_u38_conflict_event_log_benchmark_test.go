package hatReplication_test

import (
	"testing"

	"hatrie_cache/hat/hatReplication"
)

var t038ConflictBenchmarkSink hatReplication.ConflictVersion

func BenchmarkT038ConflictResolution(b *testing.B) {
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 1}
	b.Run("baseline_resolve", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			winner, err := hatReplication.ResolveConflictVersion(left, right)
			if err != nil {
				b.Fatal(err)
			}
			t038ConflictBenchmarkSink = winner
		}
	})
	b.Run("opt_in_recorded_resolve", func(b *testing.B) {
		log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 1024, HashSalt: []byte("benchmark-salt")})
		if err != nil {
			b.Fatal(err)
		}
		registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			winner, err := registry.ResolveAndRecord(log, "orders", "key", left, right)
			if err != nil {
				b.Fatal(err)
			}
			t038ConflictBenchmarkSink = winner
		}
	})
}

func BenchmarkT038ConflictEventSnapshot(b *testing.B) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 1024, HashSalt: []byte("benchmark-salt")})
	if err != nil {
		b.Fatal(err)
	}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 1}
	for index := 0; index < 1024; index++ {
		if _, err := registry.ResolveAndRecord(log, "orders", "key", left, right); err != nil {
			b.Fatal(err)
		}
	}
	payload, err := log.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(payload)), "snapshot_bytes")
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := log.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}
