package hatriecache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestHTTPReplicatorReplicatesSetAndDeleteToOwners(t *testing.T) {
	var requests []CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests = append(requests, request)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("set replication result = %#v, want one ok target", result)
	}

	response = trie.ExecuteCommand(CacheCommandRequest{Command: "DEL", Key: "session:1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "DEL", Key: "session:1"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("delete replication result = %#v, want one ok target", result)
	}

	if len(requests) != 2 {
		t.Fatalf("replicated requests len = %d, want 2", len(requests))
	}
	if requests[0].Command != "INTERNALSET" || requests[0].Key != "session:1" || requests[0].Value == "" {
		t.Fatalf("first replicated request = %#v, want INTERNALSET with snapshot", requests[0])
	}
	if requests[1].Command != "INTERNALDEL" || requests[1].Key != "session:1" {
		t.Fatalf("second replicated request = %#v, want INTERNALDEL", requests[1])
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want last result %#v", got, result)
	}
}

func TestHTTPReplicatorSkipsWhenNotLeaderOrInternalCommand(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	trie.UpsertString("session:1", "value")
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "local node is not elected leader" {
		t.Fatalf("not leader result = %#v, want skipped not leader", result)
	}

	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "INTERNALDEL", Key: "session:1"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("internal command result = %#v, want skipped internal command", result)
	}
}

func TestReplicationEndpointNormalizesAddresses(t *testing.T) {
	got, err := replicationEndpoint("127.0.0.1:8080/base/")
	if err != nil {
		t.Fatalf("replicationEndpoint() error = %v", err)
	}
	if got != "http://127.0.0.1:8080/base/api/commands" {
		t.Fatalf("endpoint = %q, want normalized API path", got)
	}
}

func replicationTestTopology(t *testing.T, replicaAddress string) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: replicaAddress},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}
