package hatCache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestExecuteCacheCommandEnforcesWriteQuorum(t *testing.T) {
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
	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:command", Value: "value"}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if !rejected {
		t.Fatalf("executeCacheCommand() rejected = false, want quorum rejection")
	}
	if response.OK {
		t.Fatalf("executeCacheCommand() response = %#v, want failed quorum response", response)
	}
	if !strings.Contains(response.Message, hatReplication.ErrWriteQuorumUnsatisfied.Error()) {
		t.Fatalf("executeCacheCommand() message = %q, want %q", response.Message, hatReplication.ErrWriteQuorumUnsatisfied)
	}
	if got := trie.GetString(request.Key); got != request.Value {
		t.Fatalf("local write = %q, want %q", got, request.Value)
	}
}

func TestExecuteCacheCommandAcceptsSatisfiedWriteQuorum(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)

	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:success", Value: "value"}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want successful quorum", rejected, response)
	}
}

func TestExecuteCacheCommandKeepsWriteQuorumDisabledByDefault(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)

	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:default", Value: "value"}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Replicator: replicator,
	})
	if rejected || !response.OK {
		t.Fatalf("default executeCacheCommand() rejected = %t, response = %#v; want legacy success", rejected, response)
	}
}

func TestExecuteCacheCommandRejectsAsynchronousWriteQuorumBeforeMutation(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{AsyncQueueSize: 1})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:async-command", Value: "value"}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if !rejected || response.OK {
		t.Fatalf("async executeCacheCommand() rejected = %t, response = %#v; want rejected", rejected, response)
	}
	if got := trie.GetString(request.Key); got != "" {
		t.Fatalf("async quorum local write = %q, want no mutation", got)
	}
}

func TestWriteQuorumOptionsDefaultOff(t *testing.T) {
	if got := (MonitoringOptions{}).WriteQuorum; got != 0 {
		t.Fatalf("MonitoringOptions.WriteQuorum = %d, want default 0", got)
	}
	if got := (CacheGRPCOptions{}).WriteQuorum; got != 0 {
		t.Fatalf("CacheGRPCOptions.WriteQuorum = %d, want default 0", got)
	}
}

func TestMonitoringHandlerEnforcesConfiguredWriteQuorum(t *testing.T) {
	ht := newTestTrie(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:    "node-a",
		Topology:    topology,
		Election:    election,
		Replicator:  replicator,
		WriteQuorum: 2,
	}).Handler()

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"quorum:http","value":"value"}`)))
	if response.Code != http.StatusConflict {
		t.Fatalf("command status = %d, want %d", response.Code, http.StatusConflict)
	}
	if got := ht.GetString("quorum:http"); got != "value" {
		t.Fatalf("local write = %q, want value after quorum failure", got)
	}
}

func newCommandQuorumTestReplicator(t testing.TB, target *httptest.Server) *HTTPReplicator {
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
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)
	return replicator
}

func BenchmarkExecuteCacheCommandWriteQuorumDisabled(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:benchmark", Value: "value"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{})
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() rejected = %t, response = %#v", rejected, response)
		}
	}
}

func BenchmarkExecuteCacheCommandWriteQuorumLoopback(b *testing.B) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	b.Cleanup(target.Close)
	replicator := newCommandQuorumTestReplicator(b, target)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := CacheCommandRequest{Command: "SETSTR", Key: "quorum:benchmark", Value: "value"}
	options := commandExecutionOptions{Replicator: replicator, WriteQuorum: 2}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, options)
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() rejected = %t, response = %#v", rejected, response)
		}
	}
}
