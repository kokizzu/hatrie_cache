package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHTTPReplicatorAsyncReportsPerTargetSequenceLag(t *testing.T) {
	requests := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- struct{}{}
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
	})
	defer replicator.Close()
	defer releaseOnce.Do(func() { close(release) })

	write := CacheCommandRequest{Command: "SETSTR", Key: "lag:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}
	waitUntil(t, time.Second, func() bool {
		replicator.mu.RLock()
		stats := replicator.queueStatsLocked()
		replicator.mu.RUnlock()
		return stats.SourceSequence == 1 && stats.ReplicationLagByTarget["node-b"] == 1
	})
	releaseOnce.Do(func() { close(release) })

	waitUntil(t, time.Second, func() bool {
		last := replicator.LastResult()
		return last.Queue != nil && last.Queue.Successes == 1
	})
	last := replicator.LastResult()
	if last.Queue == nil {
		t.Fatal("LastResult().Queue = nil, want async queue telemetry")
	}
	if last.Queue.SourceSequence != 1 {
		t.Fatalf("source sequence = %d, want 1", last.Queue.SourceSequence)
	}
	if got := last.Queue.LastAcknowledgedSequenceByTarget["node-b"]; got != 1 {
		t.Fatalf("last acknowledged sequence = %d, want 1", got)
	}
	if got := last.Queue.ReplicationLagByTarget["node-b"]; got != 0 {
		t.Fatalf("replication lag = %d, want 0", got)
	}
	var metrics strings.Builder
	writePrometheusReplicationLag(&metrics, "node-a", *last.Queue)
	for _, expected := range []string{
		"hatrie_cache_replication_source_sequence{node=\"node-a\"} 1",
		"hatrie_cache_replication_target_last_acknowledged_sequence{node=\"node-a\",target=\"node-b\"} 1",
		"hatrie_cache_replication_target_lag{node=\"node-a\",target=\"node-b\"} 0",
	} {
		if !strings.Contains(metrics.String(), expected) {
			t.Fatalf("Prometheus output missing %q:\n%s", expected, metrics.String())
		}
	}
}
