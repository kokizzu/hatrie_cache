package hatReplication_test

import (
	"errors"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestSelectReadReplicaFastPathPreservesEligibilityAndNormalization(t *testing.T) {
	candidate := []hatReplication.ReadReplicaProgress{{
		Node:        " node-a ",
		Region:      " asia ",
		Frontier:    10,
		HealthScore: 7,
	}}
	selected, err := hatReplication.SelectReadReplicaWithConsistency(
		candidate,
		hatReplication.ReadReplicaPolicy{
			ObservedFrontier: 10,
			RequiredFrontier: 9,
			MaxLag:           0,
			PreferredRegions: []string{"asia"},
		},
		hatReplication.ReadConsistencyReadAfterWrite,
	)
	if err != nil {
		t.Fatalf("single-candidate selection error = %v", err)
	}
	if selected.Node != "node-a" || selected.Region != " asia " || selected.Frontier != 10 || selected.HealthScore != 7 {
		t.Fatalf("single-candidate selection = %#v", selected)
	}
	if candidate[0].Node != " node-a " {
		t.Fatalf("candidate was mutated to %q", candidate[0].Node)
	}

	if _, err := hatReplication.SelectReadReplicaWithConsistency(
		[]hatReplication.ReadReplicaProgress{{Node: "stale", Frontier: 8}},
		hatReplication.ReadReplicaPolicy{ObservedFrontier: 10, MaxLag: 1},
		hatReplication.ReadConsistencyBoundedStaleness,
	); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("stale single candidate error = %v, want ErrNoEligibleReadReplica", err)
	}
}

var readReplicaSelectionFastPathSink hatReplication.ReadReplicaProgress

func BenchmarkSelectReadReplicaC213(b *testing.B) {
	for _, count := range []int{1, 4, 1024} {
		b.Run("candidates_"+strconv.Itoa(count), func(b *testing.B) {
			candidates := makeReadReplicaSelectionCandidates(count, false)
			policy := hatReplication.ReadReplicaPolicy{ObservedFrontier: uint64(count - 1), MaxLag: uint64(count)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				selected, err := hatReplication.SelectReadReplica(candidates, policy)
				if err != nil {
					b.Fatal(err)
				}
				readReplicaSelectionFastPathSink = selected
			}
		})
	}
	b.Run("preferred_1024", func(b *testing.B) {
		candidates := makeReadReplicaSelectionCandidates(1024, true)
		policy := hatReplication.ReadReplicaPolicy{
			ObservedFrontier: uint64(len(candidates) - 1),
			MaxLag:           uint64(len(candidates)),
			PreferredRegions: []string{"asia", "us"},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			selected, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyBoundedStaleness)
			if err != nil {
				b.Fatal(err)
			}
			readReplicaSelectionFastPathSink = selected
		}
	})
}

func makeReadReplicaSelectionCandidates(count int, withRegions bool) []hatReplication.ReadReplicaProgress {
	candidates := make([]hatReplication.ReadReplicaProgress, count)
	for index := range candidates {
		region := "us"
		if withRegions && index%2 == 0 {
			region = "asia"
		}
		candidates[index] = hatReplication.ReadReplicaProgress{
			Node:        "node-" + strconv.Itoa(index),
			Region:      region,
			Frontier:    uint64(index),
			HealthScore: index,
		}
	}
	return candidates
}
