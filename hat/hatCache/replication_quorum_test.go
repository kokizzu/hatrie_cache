package hatCache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestReplicateCommandWithQuorumCountsLocalAndRemoteAcknowledgements(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if normalizedCommand(request.Command) != replicationSetCompactCommand {
			t.Fatalf("replication command = %q, want %s", request.Command, replicationSetCompactCommand)
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: target.URL},
			{ID: "node-c", Address: target.URL},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	trie.UpsertString("quorum:key", "value")
	result, err := replicator.ReplicateCommandWithQuorum(
		context.Background(),
		trie,
		CacheCommandRequest{Command: "SETSTR", Key: "quorum:key", Value: "value"},
		CacheCommandResponse{OK: true},
		2,
	)
	if err != nil {
		t.Fatalf("ReplicateCommandWithQuorum() error = %v", err)
	}
	if result.Quorum == nil {
		t.Fatalf("ReplicateCommandWithQuorum() result = %#v, want quorum decision", result)
	}
	if got, want := result.Quorum.Total, 3; got != want {
		t.Fatalf("quorum total = %d, want %d", got, want)
	}
	if got, want := result.Quorum.Acknowledged, 3; got != want {
		t.Fatalf("quorum acknowledgements = %d, want %d", got, want)
	}
	if !result.Quorum.Satisfied {
		t.Fatalf("quorum decision = %#v, want satisfied", result.Quorum)
	}
	if got, want := len(result.Targets), 2; got != want {
		t.Fatalf("replication targets = %d, want %d", got, want)
	}
}

func TestReplicateCommandWithQuorumRejectsAsynchronousReplication(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{AsyncQueueSize: 1})
	t.Cleanup(replicator.Close)

	result, err := replicator.ReplicateCommandWithQuorum(
		context.Background(),
		newTestTrie(t),
		CacheCommandRequest{Command: "SETSTR", Key: "quorum:async", Value: "value"},
		CacheCommandResponse{OK: true},
		1,
	)
	if !errors.Is(err, hatReplication.ErrWriteQuorumAsynchronous) {
		t.Fatalf("ReplicateCommandWithQuorum() error = %v, want asynchronous replication error", err)
	}
	if !result.Skipped || result.Quorum != nil {
		t.Fatalf("async quorum result = %#v, want skipped without decision", result)
	}
}

func TestReplicateCommandWithQuorumReturnsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{})
	result, err := replicator.ReplicateCommandWithQuorum(
		ctx,
		newTestTrie(t),
		CacheCommandRequest{Command: "SETSTR", Key: "quorum:canceled", Value: "value"},
		CacheCommandResponse{OK: true},
		1,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReplicateCommandWithQuorum() error = %v, want context cancellation", err)
	}
	if !result.Skipped || result.Reason != context.Canceled.Error() {
		t.Fatalf("canceled quorum result = %#v, want skipped cancellation", result)
	}
}

func TestReplicateCommandWithQuorumReportsUnsatisfiedRemoteAcknowledgement(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()

	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: target.URL},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	trie.UpsertString("quorum:failure", "value")
	result, err := replicator.ReplicateCommandWithQuorum(
		context.Background(),
		trie,
		CacheCommandRequest{Command: "SETSTR", Key: "quorum:failure", Value: "value"},
		CacheCommandResponse{OK: true},
		2,
	)
	if !errors.Is(err, hatReplication.ErrWriteQuorumUnsatisfied) {
		t.Fatalf("ReplicateCommandWithQuorum() error = %v, want unsatisfied quorum error", err)
	}
	if result.Quorum == nil {
		t.Fatalf("quorum result = %#v, want decision", result)
	}
	if got, want := result.Quorum.Total, 2; got != want {
		t.Fatalf("quorum total = %d, want %d", got, want)
	}
	if got, want := result.Quorum.Acknowledged, 1; got != want {
		t.Fatalf("quorum acknowledgements = %d, want %d", got, want)
	}
	if result.Quorum.Satisfied {
		t.Fatalf("quorum decision = %#v, want unsatisfied", result.Quorum)
	}
}

func TestReplicateCommandWithQuorumRejectsImpossibleRequirementBeforeWrite(t *testing.T) {
	var requests int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: target.URL},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	trie.UpsertString("quorum:invalid", "value")
	result, err := replicator.ReplicateCommandWithQuorum(
		context.Background(),
		trie,
		CacheCommandRequest{Command: "SETSTR", Key: "quorum:invalid", Value: "value"},
		CacheCommandResponse{OK: true},
		3,
	)
	if !errors.Is(err, hatReplication.ErrWriteQuorumInvalid) {
		t.Fatalf("ReplicateCommandWithQuorum() error = %v, want invalid quorum error", err)
	}
	if result.Quorum != nil || requests != 0 {
		t.Fatalf("impossible quorum result = %#v, requests = %d; want no decision and no write", result, requests)
	}
}

func TestCloneReplicationResultCopiesQuorumDecision(t *testing.T) {
	original := hatReplication.WriteQuorumDecision{Total: 2, Acknowledged: 2, Required: 2, Satisfied: true}
	cloned := cloneReplicationResult(ReplicationResult{Quorum: &original})
	if cloned.Quorum == nil {
		t.Fatal("cloned quorum = nil, want decision")
	}
	cloned.Quorum.Acknowledged = 1
	if original.Acknowledged != 2 {
		t.Fatalf("original quorum = %#v, changed through clone", original)
	}
}
