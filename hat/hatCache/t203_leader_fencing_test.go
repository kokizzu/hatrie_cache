package hatCache

import (
	"context"
	"strings"
	"testing"
)

func TestRejectNonLeaderWriteRequiresCurrentFencingToken(t *testing.T) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(store, ElectionOptions{})
	request := CacheCommandRequest{
		Command: "SETSTR",
		Key:     "key",
		Value:   "value",
		Pairs:   Map{replicationMetaFencingToken: "6"},
	}

	response, rejected := rejectNonLeaderWrite(request, "node-a", store, election, true, true)
	if !rejected || response.OK || !strings.Contains(response.Message, "leader fencing token mismatch") {
		t.Fatalf("stale token response = %#v rejected=%v, want fencing rejection", response, rejected)
	}

	request.Pairs[replicationMetaFencingToken] = "7"
	response, rejected = rejectNonLeaderWrite(request, "node-a", store, election, true, true)
	if rejected || response.OK {
		t.Fatalf("current token response = %#v rejected=%v, want admission", response, rejected)
	}
}

func TestRejectNonLeaderWriteRequiresTokenWhenFencingEnabled(t *testing.T) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(store, ElectionOptions{})
	request := CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"}

	response, rejected := rejectNonLeaderWrite(request, "node-a", store, election, true, true)
	if !rejected || response.OK || !strings.Contains(response.Message, "leader fencing token is required") {
		t.Fatalf("missing token response = %#v rejected=%v, want required-token rejection", response, rejected)
	}
}

func TestExecuteCacheCommandFencingRejectsBeforeMutation(t *testing.T) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	trie := newTestTrie(t)
	request := CacheCommandRequest{
		Command: "SETSTR",
		Key:     "fenced:key",
		Value:   "value",
		Pairs:   Map{replicationMetaFencingToken: "6"},
	}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		NodeName:             "node-a",
		Topology:             store,
		Election:             NewElectionStore(store, ElectionOptions{}),
		EnforceLeaderWrites:  true,
		EnforceLeaderFencing: true,
	})
	if !rejected || response.OK {
		t.Fatalf("stale execute response = %#v rejected=%v, want rejection", response, rejected)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "fenced:key"}); got.Message != "key not found" {
		t.Fatalf("stale writer mutated trie: %#v", got)
	}
}

func TestExecuteCacheCommandStrictReplicationFencingRejectsMissingToken(t *testing.T) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "fenced:key",
		Value:   "value",
	}, commandExecutionOptions{
		Topology:             store,
		ReplicationSafety:    NewReplicationSafetyStore(),
		EnforceLeaderFencing: true,
	})
	if !rejected || response.OK || !strings.Contains(response.Message, "replication fencing token is required") {
		t.Fatalf("missing replication token response = %#v rejected=%v", response, rejected)
	}
}

func TestRejectNonLeaderWriteFencingIsOptIn(t *testing.T) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	response, rejected := rejectNonLeaderWrite(
		CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"},
		"node-a", store, NewElectionStore(store, ElectionOptions{}), true, false,
	)
	if rejected || response.OK {
		t.Fatalf("default fencing response = %#v rejected=%v, want legacy admission", response, rejected)
	}
}
