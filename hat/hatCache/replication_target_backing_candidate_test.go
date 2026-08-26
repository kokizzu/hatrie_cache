package hatCache

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newReplicationRoutingSnapshotPackedTargetsCandidate(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
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
	var targetBacking []TopologyNode
	for index, shard := range snapshot.shards {
		if index%4 == 0 {
			end := min(index+4, len(snapshot.shards))
			targetCapacity := 0
			for _, groupedShard := range snapshot.shards[index:end] {
				if groupedShard.Primary != "" {
					targetCapacity++
				}
				targetCapacity += len(groupedShard.Replicas)
			}
			targetBacking = make([]TopologyNode, 0, targetCapacity)
		}
		candidates := routeOwners(shard)
		targetStart := len(targetBacking)
		targetBacking = appendNormalizedReplicationTargetsCandidate(targetBacking, candidates, topology.Nodes, snapshot.inactive, snapshot.self)
		snapshot.targets[index] = targetBacking[targetStart:len(targetBacking):len(targetBacking)]
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

func appendNormalizedReplicationTargetsCandidate(targets []TopologyNode, owners []string, nodes []TopologyNode, inactive map[string]bool, self string) []TopologyNode {
	start := len(targets)
	for _, nodeID := range owners {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || nodeID == self || inactive[nodeID] {
			continue
		}
		node, ok := normalizedTopologyNode(nodes, nodeID)
		if !ok {
			continue
		}
		targets = append(targets, node)
	}
	added := targets[start:]
	sort.Slice(added, func(i, j int) bool {
		return added[i].ID < added[j].ID
	})
	return targets
}

func TestReplicationRoutingSnapshotPackedTargetsCandidateMatchesPerShardBacking(t *testing.T) {
	for _, state := range replicationRoutingLivenessStates {
		for _, size := range []int{2, 4, 16, 64} {
			store, election, _ := replicationRoutingLivenessFixture(t, size, state)
			baseline, baselineOK := newReplicationRoutingSnapshot("node-000", store, election)
			candidate, candidateOK := newReplicationRoutingSnapshotPackedTargetsCandidate("node-000", store, election)
			if candidateOK != baselineOK || !candidateOK {
				t.Fatalf("%s %d-shard snapshot ok = %v/%v", state, size, candidateOK, baselineOK)
			}
			if !reflect.DeepEqual(candidate.topology, baseline.topology) || !reflect.DeepEqual(candidate.shards, baseline.shards) || !reflect.DeepEqual(candidate.inactive, baseline.inactive) || !reflect.DeepEqual(candidate.leaders, baseline.leaders) || !reflect.DeepEqual(candidate.targets, baseline.targets) || candidate.self != baseline.self || candidate.fingerprint != baseline.fingerprint {
				t.Fatalf("%s %d-shard candidate state = %#v, baseline %#v", state, size, candidate, baseline)
			}
			for index, targets := range candidate.targets {
				if cap(targets) != len(targets) {
					t.Fatalf("%s %d-shard targets[%d] cap = %d, want immutable len %d", state, size, index, cap(targets), len(targets))
				}
			}
			for keyIndex := 0; keyIndex < 4096; keyIndex++ {
				key := "session:" + strconv.Itoa(keyIndex)
				want, wantTargets, wantOK := baseline.routeForKeyAndTargets(key)
				got, gotTargets, gotOK := candidate.routeForKeyAndTargets(key)
				if gotOK != wantOK || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotTargets, wantTargets) {
					t.Fatalf("%s %d-shard route(%q) = %#v/%#v/%v, want %#v/%#v/%v", state, size, key, got, gotTargets, gotOK, want, wantTargets, wantOK)
				}
			}
		}
	}
}

func BenchmarkReplicationRoutingPackedTargetsConstructionAlternating(b *testing.B) {
	for _, state := range []replicationRoutingLivenessState{replicationRoutingLivenessUntrackedHealthy, replicationRoutingLivenessOffline} {
		b.Run(string(state), func(b *testing.B) {
			for _, size := range []int{2, 4, 8, 16, 32, 64} {
				b.Run(strconv.Itoa(size)+"Shards", func(b *testing.B) {
					store, election, _ := replicationRoutingLivenessFixture(b, size, state)
					var baselineDuration, candidateDuration time.Duration
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						candidateFirst := iteration&1 != 0
						for pass := 0; pass < 2; pass++ {
							started := time.Now()
							var snapshot replicationRoutingSnapshot
							var ok bool
							if candidateFirst == (pass == 0) {
								snapshot, ok = newReplicationRoutingSnapshotPackedTargetsCandidate("node-000", store, election)
								candidateDuration += time.Since(started)
							} else {
								snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
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
				})
			}
		})
	}
}

func BenchmarkReplicationRoutingPackedTargetsConstruction(b *testing.B) {
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
							snapshot, ok = newReplicationRoutingSnapshotPackedTargetsCandidate("node-000", store, election)
						} else {
							snapshot, ok = newReplicationRoutingSnapshot("node-000", store, election)
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
}
