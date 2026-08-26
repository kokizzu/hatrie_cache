package hatCache

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
)

var benchmarkPreparedInternalReplicationOperationsSink []*snapshotOperation

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
	benchmarkInternalReplicationBatchApply(b, payloads)
}

func BenchmarkInternalReplicationDefaultBatchApply(b *testing.B) {
	const batchItems = DefaultReplicationGRPCLiveBatchMaxCommands
	payloads := make([]CacheCommandRequest, 0, batchItems)
	for idx := 0; idx < batchItems; idx++ {
		payloads = append(payloads, CacheCommandRequest{
			Command: "INTERNALSET",
			Key:     "bench:" + strconv.Itoa(idx),
			Value:   `{"type":"string","string":"value"}`,
		})
	}
	benchmarkInternalReplicationBatchApply(b, payloads)
}

func BenchmarkInternalReplicationDeleteBatchApply(b *testing.B) {
	const batchItems = 128
	payloads := make([]CacheCommandRequest, 0, batchItems)
	for idx := 0; idx < batchItems; idx++ {
		payloads = append(payloads, CacheCommandRequest{
			Command: "INTERNALDEL",
			Key:     "bench:" + strconv.Itoa(idx),
		})
	}
	benchmarkInternalReplicationBatchApply(b, payloads)
}

func BenchmarkInternalReplicationMixedBatchApply(b *testing.B) {
	const batchItems = 128
	payloads := make([]CacheCommandRequest, 0, batchItems)
	for idx := 0; idx < batchItems; idx++ {
		request := CacheCommandRequest{Command: "INTERNALDEL", Key: "bench:" + strconv.Itoa(idx)}
		if idx&1 == 0 {
			request.Command = "INTERNALSET"
			request.Value = `{"type":"string","string":"value"}`
		}
		payloads = append(payloads, request)
	}
	benchmarkInternalReplicationBatchApply(b, payloads)
}

func benchmarkInternalReplicationBatchApply(b *testing.B, payloads []CacheCommandRequest) {
	b.Helper()
	request := CacheCommandRequest{Command: "INTERNALBATCH", Batch: payloads}
	trie := CreateHatTrie()
	defer trie.Destroy()
	options := commandExecutionOptions{ReplicationSafety: NewReplicationSafetyStore()}

	b.ReportAllocs()
	b.ReportMetric(float64(len(payloads)), "items/op")
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, options)
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() = %#v rejected=%v, want ok", response, rejected)
		}
	}
}

func BenchmarkPreparedInternalReplicationOperationStorage(b *testing.B) {
	for _, count := range []int{2, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.Run("Individual", func(b *testing.B) {
				b.ReportAllocs()
				for idx := 0; idx < b.N; idx++ {
					operations := make([]*snapshotOperation, count)
					for operationIdx := range operations {
						operations[operationIdx] = new(snapshotOperation)
					}
					benchmarkPreparedInternalReplicationOperationsSink = operations
				}
			})
			b.Run("Contiguous", func(b *testing.B) {
				b.ReportAllocs()
				for idx := 0; idx < b.N; idx++ {
					backing := make([]snapshotOperation, count)
					operations := make([]*snapshotOperation, count)
					for operationIdx := range operations {
						operations[operationIdx] = &backing[operationIdx]
					}
					benchmarkPreparedInternalReplicationOperationsSink = operations
				}
			})
		})
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
