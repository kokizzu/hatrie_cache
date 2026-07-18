package hatriecache

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkInternalReplicationBatchApply(b *testing.B) {
	const batchItems = 128
	payloads := make([]CacheCommandRequest, 0, batchItems)
	for idx := 0; idx < batchItems; idx++ {
		payloads = append(payloads, CacheCommandRequest{
			Command: "INTERNALSET",
			Key:     "bench:" + strconv.Itoa(idx),
			Value:   `{"type":"string","string":"value"}`,
		})
	}
	request := CacheCommandRequest{Command: "INTERNALBATCH", Batch: payloads}
	trie := CreateHatTrie()
	defer trie.Destroy()
	options := commandExecutionOptions{ReplicationSafety: NewReplicationSafetyStore()}

	b.ReportAllocs()
	b.ReportMetric(float64(batchItems), "items/op")
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, options)
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() = %#v rejected=%v, want ok", response, rejected)
		}
	}
}

func BenchmarkPublicScalarBatchNoRemoteReplicator(b *testing.B) {
	const batchItems = 128
	payloads := make([]CacheCommandRequest, 0, batchItems)
	for idx := 0; idx < batchItems; idx++ {
		payloads = append(payloads, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "bench:" + strconv.Itoa(idx),
			Value:   "value",
		})
	}
	request := CacheCommandRequest{Command: "BATCH", Batch: payloads}
	trie := CreateHatTrie()
	defer trie.Destroy()
	topology, err := NewTopologyStore(SingleNodeTopology("node-a", ""))
	if err != nil {
		b.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
	})
	options := commandExecutionOptions{Replicator: replicator}

	b.ReportAllocs()
	b.ReportMetric(float64(batchItems), "items/op")
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, options)
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() = %#v rejected=%v, want ok", response, rejected)
		}
	}
}
