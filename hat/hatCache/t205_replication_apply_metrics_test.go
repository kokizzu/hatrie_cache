package hatCache

import (
	"strings"
	"testing"
	"time"
)

func TestT205PrometheusReplicationApplyMetrics(t *testing.T) {
	metrics := replicationMetrics{}
	base := time.Unix(100, 0)
	metrics.ObserveTargetApply("node-b", 7, 3, 120, base)
	metrics.ObserveTargetApply("node-b", 8, 2, 80, base.Add(time.Second))

	var builder strings.Builder
	writePrometheusReplicationMetrics(&builder, "node-a", metrics.Snapshot())
	output := builder.String()
	for _, want := range []string{
		`hatrie_cache_replication_target_apply_batches_total{node="node-a",target="node-b"} 2`,
		`hatrie_cache_replication_target_apply_entries_total{node="node-a",target="node-b"} 5`,
		`hatrie_cache_replication_target_apply_payload_bytes_total{node="node-a",target="node-b"} 200`,
		`hatrie_cache_replication_target_last_applied_sequence{node="node-a",target="node-b"} 8`,
		`hatrie_cache_replication_target_apply_entries_per_second{node="node-a",target="node-b"} 5`,
		`hatrie_cache_replication_target_apply_payload_bytes_per_second{node="node-a",target="node-b"} 200`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("Prometheus output missing %q:\n%s", want, output)
		}
	}
}

func TestT205AsyncAttemptFeedsTargetApplyMetrics(t *testing.T) {
	replicator := &HTTPReplicator{queue: make(chan replicationJob, 1)}
	job := replicationJob{
		tasks: []replicationTask{{
			target:       TopologyNode{ID: "node-b"},
			payload:      CacheCommandRequest{Pairs: Map{replicationMetaSequence: "11"}},
			payloadBytes: 100,
		}, {
			target:       TopologyNode{ID: "node-b"},
			payload:      CacheCommandRequest{Pairs: Map{replicationMetaSequence: "11"}},
			payloadBytes: 60,
		}},
	}
	result := ReplicationResult{
		Entries: 2,
		Targets: []ReplicationTargetResult{{Node: "node-b", OK: true}},
	}
	replicator.recordAsyncAttempt(job, result, false)
	replicator.recordAsyncAttempt(job, result, false)

	apply := replicator.MetricsSnapshot().TargetApply["node-b"]
	if apply.LastAppliedSequence != 11 || apply.AppliedBatches != 1 || apply.AppliedEntries != 2 || apply.AppliedPayloadBytes != 160 {
		t.Fatalf("target apply metrics = %#v, want one deduplicated batch", apply)
	}
}
