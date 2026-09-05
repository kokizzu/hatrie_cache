package hatCache

import (
	"reflect"
	"testing"
)

var benchmarkReplicationVectorClockSink map[string]uint64

func BenchmarkReplicationVectorClockSnapshot(b *testing.B) {
	topology := ClusterTopology{Version: clusterTopologyVersion, Mode: TopologyModeFullReplica}
	for index := 0; index < 16; index++ {
		node := "node-" + string(rune('a'+index))
		topology.Nodes = append(topology.Nodes, TopologyNode{ID: node, Address: "http://" + node})
	}
	store, err := NewTopologyStore(topology)
	if err != nil {
		b.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := &HTTPReplicator{
		self:     "node-a",
		topology: store,
		queueStats: ReplicationQueueStats{
			SourceSequence:                   100,
			LastAcknowledgedSequenceByTarget: map[string]uint64{"node-b": 97, "node-c": 88},
		},
	}
	stats := replicator.queueStats
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		benchmarkReplicationVectorClockSink = replicator.replicationVectorClockLocked(stats)
	}
}

func BenchmarkReplicationQueueStatsCloneControl(b *testing.B) {
	stats := ReplicationQueueStats{
		SourceSequence:                   100,
		LastAcknowledgedSequenceByTarget: map[string]uint64{"node-b": 97, "node-c": 88},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		benchmarkReplicationVectorClockSink = cloneReplicationQueueStats(stats).LastAcknowledgedSequenceByTarget
	}
}

func TestReplicationQueueStatsExposeVectorClockForEveryReplica(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: clusterTopologyVersion,
		Mode:    TopologyModeFullReplica,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
			{ID: "node-c", Address: "http://node-c"},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := &HTTPReplicator{
		self:     "node-a",
		topology: topology,
		queue:    make(chan replicationJob, 1),
		queueStats: ReplicationQueueStats{
			Enabled:                          true,
			SourceSequence:                   12,
			LastAcknowledgedSequenceByTarget: map[string]uint64{"node-b": 7},
		},
	}

	result := replicator.LastResult()
	if result.Queue == nil {
		t.Fatal("LastResult().Queue = nil, want queue statistics")
	}
	stats := *result.Queue
	want := map[string]uint64{"node-a": 12, "node-b": 7, "node-c": 0}
	if !reflect.DeepEqual(stats.VectorClock, want) {
		t.Fatalf("VectorClock = %#v, want %#v", stats.VectorClock, want)
	}
	stats.VectorClock["node-a"] = 99
	if got := replicator.LastResult().Queue.VectorClock["node-a"]; got != 12 {
		t.Fatalf("VectorClock exposed internal state: got node-a=%d, want 12", got)
	}
}
