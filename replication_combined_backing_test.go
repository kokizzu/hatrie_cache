package hatriecache

import (
	"strconv"
	"testing"
	"time"
)

func newReplicationRoutingSnapshotSeparateTargetControl(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
	if topologyStore == nil {
		return replicationRoutingSnapshot{}, false
	}
	topology, fingerprint := topologyStore.replicationRoutingGeneration()
	snapshot := replicationRoutingSnapshot{
		topology:    topology,
		self:        self,
		fingerprint: fingerprint,
	}
	if election != nil {
		snapshot.inactive = election.inactiveNodesSnapshot(topology)
	}
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := fullReplicaShard(topology)
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
	if len(snapshot.shards) == 1 {
		shard := snapshot.shards[0]
		candidates := routeOwners(shard)
		snapshot.targets[0] = precomputedNormalizedReplicationTargets(candidates, topology.Nodes, snapshot.inactive, snapshot.self)
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
		snapshot.leaders[0] = leader
		return snapshot, true
	}
	for groupStart := 0; groupStart < len(snapshot.shards); groupStart += replicationOwnerBackingGroupSize {
		groupEnd := min(groupStart+replicationOwnerBackingGroupSize, len(snapshot.shards))
		ownerCount := 0
		for _, shard := range snapshot.shards[groupStart:groupEnd] {
			if shard.Primary != "" {
				ownerCount++
			}
			ownerCount += len(shard.Replicas)
		}
		ownerBacking := make([]string, 0, ownerCount)
		for index := groupStart; index < groupEnd; index++ {
			shard := snapshot.shards[index]
			ownerStart := len(ownerBacking)
			if shard.Primary != "" {
				ownerBacking = append(ownerBacking, shard.Primary)
			}
			ownerBacking = append(ownerBacking, shard.Replicas...)
			candidates := ownerBacking[ownerStart:len(ownerBacking):len(ownerBacking)]
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
	}
	return snapshot, true
}

func TestReplicationRoutingSnapshotCombinedBackingMatchesSeparateTargets(t *testing.T) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 16, 64} {
			store, election, _ := replicationRoutingLivenessFixture(t, size, state)
			baseline, baselineOK := newReplicationRoutingSnapshotSeparateTargetControl("node-000", store, election)
			candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
			compareReplicationRoutingSnapshots(t, string(state)+"/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
			for index, targets := range candidate.targets {
				if cap(targets) != len(targets) {
					t.Fatalf("%s/%d targets %d capacity = %d, len %d", state, size, index, cap(targets), len(targets))
				}
			}
		}
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		store, election := replicationRoutingFullReplicaSortFixture(t, size)
		baseline, baselineOK := newReplicationRoutingSnapshotSeparateTargetControl("node-000", store, election)
		candidate, candidateOK := newReplicationRoutingSnapshot("node-000", store, election)
		compareReplicationRoutingSnapshots(t, "FullReplica/"+strconv.Itoa(size), baseline, baselineOK, candidate, candidateOK)
	}
}

func BenchmarkReplicationRoutingCombinedBackingConstructionAlternating(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		b.Run(string(state), func(b *testing.B) {
			for _, size := range []int{2, 4, 8, 16, 32, 64} {
				b.Run(strconv.Itoa(size)+"Shards", func(b *testing.B) {
					store, election, _ := replicationRoutingLivenessFixture(b, size, state)
					benchmarkReplicationRoutingCombinedBackingAlternating(b, store, election)
				})
			}
		})
	}
	for _, size := range []int{2, 4, 8, 16, 32, 64} {
		b.Run("FullReplica/"+strconv.Itoa(size)+"Nodes", func(b *testing.B) {
			store, election := replicationRoutingFullReplicaSortFixture(b, size)
			benchmarkReplicationRoutingCombinedBackingAlternating(b, store, election)
		})
	}
}

func benchmarkReplicationRoutingCombinedBackingAlternating(b *testing.B, store *TopologyStore, election *ElectionStore) {
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
				snapshot, ok = newReplicationRoutingSnapshotSeparateTargetControl("node-000", store, election)
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

func BenchmarkReplicationRoutingCombinedBackingConstruction(b *testing.B) {
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
							snapshot, ok = newReplicationRoutingSnapshotSeparateTargetControl("node-000", store, election)
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
						snapshot, ok = newReplicationRoutingSnapshotSeparateTargetControl("node-000", store, election)
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
