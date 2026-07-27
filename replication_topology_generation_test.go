package hatriecache

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newReplicationRoutingSnapshotClonedTopologyControl(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
	if topologyStore == nil {
		return replicationRoutingSnapshot{}, false
	}
	topology, fingerprint := topologyStore.replicationSnapshot()
	snapshot := replicationRoutingSnapshot{
		topology:    topology,
		self:        self,
		fingerprint: fingerprint,
	}
	if election != nil {
		snapshot.inactive = election.inactiveNodesSnapshot(topology)
	}
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := topology.fullReplicaShard()
		if !ok {
			return replicationRoutingSnapshot{}, false
		}
		snapshot.shards = []TopologyShard{shard}
	} else {
		snapshot.shards = topology.Shards
	}
	if len(snapshot.shards) == 0 {
		return replicationRoutingSnapshot{}, false
	}
	snapshot.leaders = make([]ElectionLeader, len(snapshot.shards))
	snapshot.targets = make([][]TopologyNode, len(snapshot.shards))
	for index, shard := range snapshot.shards {
		candidates := routeOwners(shard)
		snapshot.targets[index] = precomputedNormalizedReplicationTargets(candidates, topology.Nodes, snapshot.inactive, snapshot.self)
		leader := ElectionLeader{
			Shard:      shard.ID,
			Primary:    shard.Primary,
			Candidates: candidates,
		}
		if election == nil {
			leader.Leader = shard.Primary
			leader.Available = true
		} else {
			for _, nodeID := range candidates {
				if !snapshot.inactive[nodeID] {
					leader.Leader = nodeID
					leader.Available = true
					break
				}
			}
		}
		snapshot.leaders[index] = leader
	}
	return snapshot, true
}

func TestReplicationRoutingSnapshotBorrowedTopologyMatchesClonedGeneration(t *testing.T) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 16, 64} {
			store, election, _ := replicationRoutingLivenessFixture(t, size, state)
			baseline, baselineOK := newReplicationRoutingSnapshotClonedTopologyControl("node-000", store, election)
			candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
			compareReplicationRoutingSnapshots(t, string(state)+"/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
		}
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		store, election := replicationRoutingFullReplicaSortFixture(t, size)
		baseline, baselineOK := newReplicationRoutingSnapshotClonedTopologyControl("node-000", store, election)
		candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
		compareReplicationRoutingSnapshots(t, "FullReplica/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
	}
}

func TestReplicationRoutingSnapshotBorrowedTopologySurvivesGenerationReplacement(t *testing.T) {
	store, _, _ := replicationRoutingLivenessFixture(t, 4, replicationRoutingLivenessUntrackedHealthy)
	oldSnapshot, ok := newReplicationRoutingSnapshot("node-000", store, nil)
	if !ok {
		t.Fatal("old borrowed routing snapshot construction failed")
	}
	oldTopology := oldSnapshot.topology
	oldFingerprint := oldSnapshot.fingerprint

	next := store.Get()
	for index := range next.Nodes {
		next.Nodes[index].Address += "/next"
	}
	next.Shards[0].Primary, next.Shards[0].Replicas[0] = next.Shards[0].Replicas[0], next.Shards[0].Primary
	if err := store.Set(next); err != nil {
		t.Fatalf("Set(next generation) error = %v", err)
	}

	if !reflect.DeepEqual(oldSnapshot.topology, oldTopology) || oldSnapshot.fingerprint != oldFingerprint {
		t.Fatalf("old borrowed snapshot changed after Set: %#v/%q", oldSnapshot.topology, oldSnapshot.fingerprint)
	}
	newSnapshot, ok := newReplicationRoutingSnapshot("node-000", store, nil)
	if !ok {
		t.Fatal("new borrowed routing snapshot construction failed")
	}
	if reflect.DeepEqual(newSnapshot.topology, oldSnapshot.topology) || newSnapshot.fingerprint == oldSnapshot.fingerprint {
		t.Fatalf("new generation was not installed: %#v/%q", newSnapshot.topology, newSnapshot.fingerprint)
	}
	if got := store.Get(); !reflect.DeepEqual(got, newSnapshot.topology) {
		t.Fatalf("store generation = %#v, snapshot %#v", got, newSnapshot.topology)
	}
}

func TestReplicationRoutingSnapshotBorrowedTopologyConcurrentGenerationReplacement(t *testing.T) {
	store, _, _ := replicationRoutingLivenessFixture(t, 16, replicationRoutingLivenessUntrackedHealthy)
	topologyA := store.Get()
	topologyB := store.Get()
	for index := range topologyA.Nodes {
		topologyA.Nodes[index].Address = "http://generation-a/" + topologyA.Nodes[index].ID
		topologyB.Nodes[index].Address = "http://generation-b/" + topologyB.Nodes[index].ID
	}
	fingerprintA := topologyA.Fingerprint()
	fingerprintB := topologyB.Fingerprint()
	if err := store.Set(topologyA); err != nil {
		t.Fatalf("Set(initial generation) error = %v", err)
	}

	const iterations = 1000
	errors := make(chan error, 8)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			topology := topologyA
			if iteration&1 != 0 {
				topology = topologyB
			}
			if err := store.Set(topology); err != nil {
				errors <- fmt.Errorf("Set(generation %d): %w", iteration, err)
				return
			}
		}
	}()
	for reader := 0; reader < 4; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				snapshot, ok := newReplicationRoutingSnapshot("node-000", store, nil)
				if !ok {
					errors <- fmt.Errorf("borrowed snapshot construction failed")
					return
				}
				prefix := "http://generation-a/"
				expectedFingerprint := fingerprintA
				if snapshot.fingerprint == fingerprintB {
					prefix = "http://generation-b/"
					expectedFingerprint = fingerprintB
				}
				if snapshot.fingerprint != expectedFingerprint {
					errors <- fmt.Errorf("unexpected fingerprint %q", snapshot.fingerprint)
					return
				}
				for _, node := range snapshot.topology.Nodes {
					if node.Address != prefix+node.ID {
						errors <- fmt.Errorf("mixed generation node %#v for fingerprint %q", node, snapshot.fingerprint)
						return
					}
				}
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

func BenchmarkReplicationRoutingBorrowedTopologyConstructionAlternating(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		b.Run(string(state), func(b *testing.B) {
			for _, size := range []int{2, 4, 8, 16, 32, 64} {
				b.Run(strconv.Itoa(size)+"Shards", func(b *testing.B) {
					store, election, _ := replicationRoutingLivenessFixture(b, size, state)
					benchmarkReplicationRoutingBorrowedTopologyAlternating(b, store, election)
				})
			}
		})
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		b.Run("FullReplica/"+strconv.Itoa(size)+"Nodes", func(b *testing.B) {
			store, election := replicationRoutingFullReplicaSortFixture(b, size)
			benchmarkReplicationRoutingBorrowedTopologyAlternating(b, store, election)
		})
	}
}

func benchmarkReplicationRoutingBorrowedTopologyAlternating(b *testing.B, store *TopologyStore, election *ElectionStore) {
	var baselineDuration, candidateDuration time.Duration
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		candidateFirst := iteration&1 != 0
		for pass := 0; pass < 2; pass++ {
			started := time.Now()
			var snapshot replicationRoutingSnapshot
			var ok bool
			if candidateFirst == (pass == 0) {
				snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
				candidateDuration += time.Since(started)
			} else {
				snapshot, ok = newReplicationRoutingSnapshotClonedTopologyControl("node-000", store, election)
				baselineDuration += time.Since(started)
			}
			if !ok {
				b.Fatal("routing snapshot construction failed")
			}
			benchmarkReplicationRoutingSnapshotSink = snapshot
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(baselineDuration.Nanoseconds())/float64(b.N), "baseline_ns/snapshot")
	b.ReportMetric(float64(candidateDuration.Nanoseconds())/float64(b.N), "candidate_ns/snapshot")
}

func BenchmarkReplicationRoutingBorrowedTopologyConstruction(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		for _, size := range []int{2, 4, 8, 16, 32, 64} {
			store, election, _ := replicationRoutingLivenessFixture(b, size, state)
			for _, candidate := range []bool{false, true} {
				layout := "Baseline"
				if candidate {
					layout = "Candidate"
				}
				b.Run(string(state)+"/"+strconv.Itoa(size)+"Shards/"+layout, func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						var snapshot replicationRoutingSnapshot
						var ok bool
						if candidate {
							snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
						} else {
							snapshot, ok = newReplicationRoutingSnapshotClonedTopologyControl("node-000", store, election)
						}
						if !ok {
							b.Fatal("routing snapshot construction failed")
						}
						benchmarkReplicationRoutingSnapshotSink = snapshot
					}
				})
			}
		}
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		store, election := replicationRoutingFullReplicaSortFixture(b, size)
		for _, candidate := range []bool{false, true} {
			layout := "Baseline"
			if candidate {
				layout = "Candidate"
			}
			b.Run("FullReplica/"+strconv.Itoa(size)+"Nodes/"+layout, func(b *testing.B) {
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					var snapshot replicationRoutingSnapshot
					var ok bool
					if candidate {
						snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
					} else {
						snapshot, ok = newReplicationRoutingSnapshotClonedTopologyControl("node-000", store, election)
					}
					if !ok {
						b.Fatal("routing snapshot construction failed")
					}
					benchmarkReplicationRoutingSnapshotSink = snapshot
				}
			})
		}
	}
}
