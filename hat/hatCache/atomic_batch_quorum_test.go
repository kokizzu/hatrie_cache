package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestExecuteCacheCommandEnforcesAtomicBatchWriteQuorum(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()

	replicator := newAtomicBatchQuorumTestReplicator(t, target)
	defer replicator.Close()

	trie := newTestTrie(t)
	request := atomicBatchQuorumTestRequest()
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if !rejected {
		t.Fatalf("executeCacheCommand() rejected = false, want atomic batch quorum rejection")
	}
	if response.OK {
		t.Fatalf("executeCacheCommand() response = %#v, want failed quorum response", response)
	}
	if !strings.Contains(response.Message, "write quorum") {
		t.Fatalf("executeCacheCommand() message = %q, want write quorum failure", response.Message)
	}
	if got := trie.GetString("quorum:batch:a"); got != "one" {
		t.Fatalf("local batch value a = %q, want committed local value", got)
	}
	if got := trie.GetString("quorum:batch:b"); got != "two" {
		t.Fatalf("local batch value b = %q, want committed local value", got)
	}
}

func TestExecuteCacheCommandAcceptsAtomicBatchWriteQuorum(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		calls.Add(1)
		if request.Command != "INTERNALBATCHV2" || request.Atomic || len(request.Batch) != 2 {
			t.Errorf("replicated request = %#v, want one internal atomic batch envelope with two payloads", request)
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := newAtomicBatchQuorumTestReplicator(t, target)
	defer replicator.Close()

	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), atomicBatchQuorumTestRequest(), commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want satisfied atomic batch quorum", rejected, response)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("replication calls = %d, want one atomic batch", got)
	}
}

func TestExecuteCacheCommandRejectsAtomicBatchWriteQuorumBeforeMutation(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer target.Close()

	replicator := newAtomicBatchQuorumTestReplicatorWithQueue(t, target, 1)
	defer replicator.Close()

	trie := newTestTrie(t)
	response, rejected := executeCacheCommand(context.Background(), trie, atomicBatchQuorumTestRequest(), commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if !rejected || response.OK {
		t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want pre-mutation quorum rejection", rejected, response)
	}
	if !strings.Contains(response.Message, "synchronous") {
		t.Fatalf("executeCacheCommand() message = %q, want synchronous replication requirement", response.Message)
	}
	if got := trie.GetString("quorum:batch:a"); got != "" {
		t.Fatalf("local batch value a = %q, want no mutation", got)
	}
	if got := trie.GetString("quorum:batch:b"); got != "" {
		t.Fatalf("local batch value b = %q, want no mutation", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("replication calls = %d, want no request before quorum validation", got)
	}
}

func atomicBatchQuorumTestRequest() CacheCommandRequest {
	return CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "quorum:batch:a", Value: "one"},
			{Command: "SETSTR", Key: "quorum:batch:b", Value: "two"},
		},
	}
}

func newAtomicBatchQuorumTestReplicator(t *testing.T, target *httptest.Server) *HTTPReplicator {
	return newAtomicBatchQuorumTestReplicatorWithQueue(t, target, 0)
}

func newAtomicBatchQuorumTestReplicatorWithQueue(t *testing.T, target *httptest.Server, asyncQueueSize int) *HTTPReplicator {
	t.Helper()
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
	return NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       NewElectionStore(topology, ElectionOptions{}),
		Client:         target.Client(),
		AsyncQueueSize: asyncQueueSize,
	})
}
