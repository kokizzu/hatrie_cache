package hatriecache

import (
	"context"
	"path/filepath"
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

func BenchmarkPublicScalarBatchJournalDurability10K(b *testing.B) {
	const batchItems = 10_000
	individual := make([]CacheCommandRequest, batchItems)
	batched := make([]CacheCommandRequest, 0, (batchItems+maxPublicCommandBatchSize-1)/maxPublicCommandBatchSize)
	for first := 0; first < batchItems; first += maxPublicCommandBatchSize {
		last := first + maxPublicCommandBatchSize
		if last > batchItems {
			last = batchItems
		}
		payloads := make([]CacheCommandRequest, last-first)
		for offset := range payloads {
			idx := first + offset
			individual[idx] = CacheCommandRequest{
				Command: "SETSTR",
				Key:     "durable:" + strconv.Itoa(idx),
				Value:   "value",
			}
			payloads[offset] = individual[idx]
		}
		batched = append(batched, CacheCommandRequest{Command: "BATCH", Batch: payloads})
	}

	for _, test := range []struct {
		name     string
		requests []CacheCommandRequest
	}{
		{name: "Individual", requests: individual},
		{name: "Batch4096", requests: batched},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: 1,
			})
			if err != nil {
				b.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
			}
			b.Cleanup(func() { _ = journal.Close() })
			options := commandExecutionOptions{Journal: journal}

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				for _, request := range test.requests {
					response, rejected := executeCacheCommand(context.Background(), trie, request, options)
					if rejected || !response.OK {
						b.Fatalf("executeCacheCommand() = %#v rejected=%v, want ok", response, rejected)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(batchItems, "items/op")
			b.ReportMetric(float64(len(test.requests)), "journal_syncs/op")
		})
	}
}
