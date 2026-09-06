package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestSelectReadReplicaHonorsFreshnessAndUsesDeterministicPreference(t *testing.T) {
	replicas := []hatReplication.ReadReplicaProgress{
		{Node: "slow", Frontier: 98, HealthScore: 100},
		{Node: "healthy-low-frontier", Frontier: 99, HealthScore: 1000},
		{Node: "node-b", Frontier: 100, HealthScore: 80},
		{Node: " node-a ", Frontier: 100, HealthScore: 80},
	}
	selected, err := hatReplication.SelectReadReplica(replicas, hatReplication.ReadReplicaPolicy{ObservedFrontier: 100, MaxLag: 0})
	if err != nil {
		t.Fatalf("SelectReadReplica() error = %v", err)
	}
	if selected.Node != "node-a" {
		t.Fatalf("selected replica = %#v, want node-a lexical tie-break", selected)
	}

	selected, err = hatReplication.SelectReadReplica(replicas, hatReplication.ReadReplicaPolicy{ObservedFrontier: 100, RequiredFrontier: 99, MaxLag: 1})
	if err != nil || selected.Node != "node-a" {
		t.Fatalf("lag-bounded selection = %#v, %v, want node-a", selected, err)
	}
}

func TestSelectReadReplicaRejectsStaleOrInvalidCandidates(t *testing.T) {
	if _, err := hatReplication.SelectReadReplica(nil, hatReplication.ReadReplicaPolicy{}); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("empty selection error = %v, want ErrNoEligibleReadReplica", err)
	}
	if _, err := hatReplication.SelectReadReplica([]hatReplication.ReadReplicaProgress{{Node: "stale", Frontier: 1}}, hatReplication.ReadReplicaPolicy{ObservedFrontier: 10, MaxLag: 2}); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("stale selection error = %v, want ErrNoEligibleReadReplica", err)
	}
	if _, err := hatReplication.SelectReadReplica([]hatReplication.ReadReplicaProgress{{Node: " ", Frontier: 10}}, hatReplication.ReadReplicaPolicy{}); !errors.Is(err, hatReplication.ErrReadReplicaNameRequired) {
		t.Fatalf("invalid candidate error = %v, want ErrReadReplicaNameRequired", err)
	}
	if _, err := hatReplication.SelectReadReplica([]hatReplication.ReadReplicaProgress{{Node: "node", Frontier: 10}}, hatReplication.ReadReplicaPolicy{RequiredFrontier: 11}); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("required frontier error = %v, want ErrNoEligibleReadReplica", err)
	}
}

func TestSelectReadReplicaDoesNotMutateCandidates(t *testing.T) {
	replicas := []hatReplication.ReadReplicaProgress{{Node: " node ", Frontier: 5}}
	if _, err := hatReplication.SelectReadReplica(replicas, hatReplication.ReadReplicaPolicy{}); err != nil {
		t.Fatalf("SelectReadReplica() error = %v", err)
	}
	if replicas[0].Node != " node " {
		t.Fatalf("candidate mutated to %q", replicas[0].Node)
	}
}

func BenchmarkSelectReadReplica(b *testing.B) {
	replicas := make([]hatReplication.ReadReplicaProgress, 1024)
	for index := range replicas {
		replicas[index] = hatReplication.ReadReplicaProgress{Node: "node-" + string(rune(index)), Frontier: uint64(index), HealthScore: index}
	}
	policy := hatReplication.ReadReplicaPolicy{ObservedFrontier: 1023, MaxLag: 3}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := hatReplication.SelectReadReplica(replicas, policy); err != nil {
			b.Fatal(err)
		}
	}
}
