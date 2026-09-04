package hatCache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPReplicatorRecordsWireMetrics(t *testing.T) {
	wireBytes := map[string]int64{}
	requests := map[string]int{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding := r.Header.Get("Content-Encoding")
		if encoding == "" {
			encoding = "identity"
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll(request body) error = %v", err)
		}
		wireBytes[encoding] += int64(len(body))
		requests[encoding]++
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
		Client:   target.Client(),
	})

	commands := []CacheCommandRequest{
		{Command: "SETSTR", Key: "wire:small", Value: "small"},
		{Command: "SETSTR", Key: "wire:large", Value: strings.Repeat("x", 64*1024)},
	}
	for _, command := range commands {
		response := trie.ExecuteCommand(command)
		if !response.OK {
			t.Fatalf("ExecuteCommand(%s) response = %#v, want ok", command.Key, response)
		}
		result := replicator.ReplicateCommand(context.Background(), trie, command, response)
		if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
			t.Fatalf("ReplicateCommand(%s) result = %#v, want one successful target", command.Key, result)
		}
	}

	metrics := replicator.MetricsSnapshot()
	bytesByEncoding := metrics.TargetWireBytes["node-b"]
	requestsByEncoding := metrics.TargetWireRequests["node-b"]
	for _, encoding := range []string{"identity", "gzip"} {
		if bytesByEncoding[encoding] == 0 {
			t.Fatalf("wire bytes for %s = %d, want non-zero; all metrics = %#v", encoding, bytesByEncoding[encoding], metrics)
		}
		if requestsByEncoding[encoding] != uint64(requests[encoding]) {
			t.Fatalf("wire requests for %s = %d, want %d", encoding, requestsByEncoding[encoding], requests[encoding])
		}
		if bytesByEncoding[encoding] != uint64(wireBytes[encoding]) {
			t.Fatalf("wire bytes for %s = %d, want %d", encoding, bytesByEncoding[encoding], wireBytes[encoding])
		}
	}
}

func TestPrometheusReplicationWireMetrics(t *testing.T) {
	var builder strings.Builder
	writePrometheusReplicationMetrics(&builder, "node-a", ReplicationMetricsSnapshot{
		TargetWireBytes: map[string]map[string]uint64{
			"node-b": {"gzip": 20, "identity": 11},
		},
		TargetWireRequests: map[string]map[string]uint64{
			"node-b": {"gzip": 2, "identity": 1},
		},
	})
	output := builder.String()
	for _, want := range []string{
		"# HELP hatrie_cache_replication_request_wire_bytes_total",
		"# TYPE hatrie_cache_replication_request_wire_bytes_total counter",
		"hatrie_cache_replication_request_wire_bytes_total{node=\"node-a\",target=\"node-b\",encoding=\"gzip\"} 20",
		"hatrie_cache_replication_request_wire_requests_total{node=\"node-a\",target=\"node-b\",encoding=\"identity\"} 1",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("Prometheus output missing %q:\n%s", want, output)
		}
	}
}
