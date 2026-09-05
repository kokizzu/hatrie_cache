package hatCache

import (
	"context"
	"strings"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestCacheGRPCServerGatesUnhealthyReplicaReadFastPaths(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("health:key", "value")
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(node-b) error = %v", err)
	}
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName:                   "node-b",
		Topology:                   topology,
		Election:                   election,
		RequireHealthyReplicaReads: true,
	})
	defer stop()

	unary, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETSTR",
		Key:     "health:key",
	})
	if err != nil {
		t.Fatalf("unary GETSTR error = %v", err)
	}
	assertReplicaReadRejected(t, unary.GetOk(), unary.GetMessage())

	scalarStream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		t.Fatalf("ScalarBatchStream() error = %v", err)
	}
	if err := scalarStream.Send(&hatriecachev1.ScalarBatchRequest{
		BatchId:    201,
		Operations: []hatriecachev1.ScalarCommand{hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET},
		Keys:       []string{"health:key"},
	}); err != nil {
		t.Fatalf("ScalarBatchStream.Send(read) error = %v", err)
	}
	scalarRead, err := scalarStream.Recv()
	if err != nil {
		t.Fatalf("ScalarBatchStream.Recv(read) error = %v", err)
	}
	assertReplicaReadRejected(t, scalarRead.GetOk(), scalarRead.GetError())

	if err := scalarStream.Send(&hatriecachev1.ScalarBatchRequest{
		BatchId:     202,
		Operations:  []hatriecachev1.ScalarCommand{hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING},
		Keys:        []string{"health:write"},
		StringValues: [][]byte{[]byte("written")},
	}); err != nil {
		t.Fatalf("ScalarBatchStream.Send(write) error = %v", err)
	}
	scalarWrite, err := scalarStream.Recv()
	if err != nil {
		t.Fatalf("ScalarBatchStream.Recv(write) error = %v", err)
	}
	if !scalarWrite.GetOk() || ht.GetString("health:write") != "written" {
		t.Fatalf("scalar write response = %#v, stored = %q; want successful write", scalarWrite, ht.GetString("health:write"))
	}

	structuredStream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatalf("StructuredBatchStream() error = %v", err)
	}
	if err := structuredStream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId:    203,
		Operations: []hatriecachev1.StructuredCommand{hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP},
		Keys:       []string{"health:map"},
		Subkeys:    []string{"field"},
	}); err != nil {
		t.Fatalf("StructuredBatchStream.Send(read) error = %v", err)
	}
	structuredRead, err := structuredStream.Recv()
	if err != nil {
		t.Fatalf("StructuredBatchStream.Recv(read) error = %v", err)
	}
	assertReplicaReadRejected(t, structuredRead.GetOk(), structuredRead.GetError())

	if err := structuredStream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId:    204,
		Operations: []hatriecachev1.StructuredCommand{hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP},
		Keys:       []string{"health:map"},
		Subkeys:    []string{"field"},
		Values:     [][]byte{[]byte("written")},
	}); err != nil {
		t.Fatalf("StructuredBatchStream.Send(write) error = %v", err)
	}
	structuredWrite, err := structuredStream.Recv()
	if err != nil {
		t.Fatalf("StructuredBatchStream.Recv(write) error = %v", err)
	}
	if !structuredWrite.GetOk() {
		t.Fatalf("structured write response = %#v, want successful write", structuredWrite)
	}
	if value, ok, err := ht.PeekMapChecked("health:map", "field"); err != nil || !ok || value != "written" {
		t.Fatalf("structured write value = %q/%v/%v, want written/true/nil", value, ok, err)
	}
}

func assertReplicaReadRejected(t *testing.T, ok bool, message string) {
	t.Helper()
	if ok || !strings.Contains(message, "local replica is not healthy for stale-sensitive reads") {
		t.Fatalf("replica read response = ok %v message %q, want health rejection", ok, message)
	}
}
