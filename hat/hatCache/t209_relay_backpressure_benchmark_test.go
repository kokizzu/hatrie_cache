package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkT209ReplicationRelayBackpressureLegacyAdmission(b *testing.B) {
	job := replicationJob{tasks: []replicationTask{{target: TopologyNode{ID: "node-b"}}}}
	replicator := &HTTPReplicator{
		queueStats: ReplicationQueueStats{
			SourceSequence:                   10,
			LastAcknowledgedSequenceByTarget: map[string]uint64{"node-b": 9},
		},
	}
	allowed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		replicator.mu.RLock()
		lag := replicationSequenceLag(replicator.queueStats.SourceSequence, replicator.queueStats.LastAcknowledgedSequenceByTarget["node-b"])
		replicator.mu.RUnlock()
		if lag < 10_000 && len(job.tasks) > 0 {
			allowed++
		}
	}
	b.StopTimer()
	if allowed == 0 {
		b.Fatal("legacy relay admission admitted no work")
	}
}

func BenchmarkT209ReplicationRelayBackpressureAdmission(b *testing.B) {
	job := replicationJob{tasks: []replicationTask{{target: TopologyNode{ID: "node-b"}}}}
	replicator := &HTTPReplicator{
		relayBackpressure: hatReplication.NewRelayBackpressure(hatReplication.RelayBackpressureOptions{
			Enabled:       true,
			HighWatermark: 10_000,
		}),
		queueStats: ReplicationQueueStats{
			SourceSequence:                   10,
			LastAcknowledgedSequenceByTarget: map[string]uint64{"node-b": 9},
		},
	}
	allowed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if replicator.relayBackpressureAllows(job) {
			allowed++
		}
	}
	b.StopTimer()
	if allowed == 0 {
		b.Fatal("relay backpressure admitted no work")
	}
}
