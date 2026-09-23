package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestT209ReplicationRelayBackpressureDefaultsOff(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{AsyncQueueSize: 1})
	t.Cleanup(replicator.Close)
	if replicator.relayBackpressure != nil {
		t.Fatal("default relay backpressure is configured, want disabled")
	}
}

func TestT209ReplicationRelayBackpressureBlocksAndResumes(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		AsyncQueueSize: 1,
		AsyncRelayBackpressure: hatReplication.RelayBackpressureOptions{
			Enabled:         true,
			HighWatermark:   4,
			ResumeWatermark: 2,
		},
	})
	t.Cleanup(replicator.Close)

	replicator.mu.Lock()
	replicator.queueStats.SourceSequence = 10
	replicator.queueStats.LastAcknowledgedSequenceByTarget = map[string]uint64{"node-b": 5}
	replicator.refreshReplicationLagLocked()
	replicator.mu.Unlock()

	job := replicationJob{
		result: ReplicationResult{Command: "SETSTR", Key: "relay:key"},
		tasks:  []replicationTask{{target: TopologyNode{ID: "node-b"}}},
	}
	result := replicator.enqueueReplicationJob(job)
	if !result.Skipped || result.Reason != "replication relay backpressure is active" {
		t.Fatalf("blocked enqueue result = %#v, want relay backpressure skip", result)
	}
	replicator.mu.RLock()
	stats := replicator.queueStats
	replicator.mu.RUnlock()
	if !stats.BackpressurePaused || stats.BackpressureLag != 5 {
		t.Fatalf("blocked queue stats = %#v, want paused lag 5", stats)
	}

	replicator.mu.Lock()
	replicator.queueStats.LastAcknowledgedSequenceByTarget["node-b"] = 8
	replicator.refreshReplicationLagLocked()
	replicator.refreshRelayBackpressureLocked()
	replicator.mu.Unlock()
	if !replicator.relayBackpressureAllows(job) {
		t.Fatal("relay backpressure remained active after lag crossed resume watermark")
	}
	replicator.mu.RLock()
	stats = replicator.queueStats
	replicator.mu.RUnlock()
	if stats.BackpressurePaused || stats.BackpressureLag != 2 {
		t.Fatalf("resumed queue stats = %#v, want unpaused lag 2", stats)
	}
}
