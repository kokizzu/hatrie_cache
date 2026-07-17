package hatriecache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkHTTPReplicatorSyncAllBatching(b *testing.B) {
	const keyCount = 10000
	for _, tt := range []struct {
		name     string
		pageSize int
	}{
		{name: "Batched10k", pageSize: keyCount},
		{name: "Unbatched10k", pageSize: 1},
	} {
		b.Run(tt.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			for i := 0; i < keyCount; i++ {
				key := "session:" + strconv.Itoa(i)
				trie.UpsertString(key, "value-"+strconv.Itoa(i))
			}

			var requests atomic.Int64
			var wireBytes atomic.Int64
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/commands" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				n, err := io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				requests.Add(1)
				wireBytes.Add(n)
				writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
			}))
			b.Cleanup(target.Close)

			topology, err := NewTopologyStore(ClusterTopology{
				Version: 1,
				Self:    "node-a",
				Nodes: []TopologyNode{
					{ID: "node-a", Address: "http://127.0.0.1:1"},
					{ID: "node-b", Address: target.URL},
				},
				Shards: []TopologyShard{
					{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
				},
			})
			if err != nil {
				b.Fatalf("NewTopologyStore() error = %v", err)
			}
			election := NewElectionStore(topology, ElectionOptions{})
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: election,
				Client:   target.Client(),
			})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := replicator.syncAllPaged(context.Background(), trie, "session:", tt.pageSize)
				if result.Skipped || result.Entries != keyCount || len(result.Targets) == 0 {
					b.Fatalf("syncAllPaged() = %#v, want %d synced entries", result, keyCount)
				}
				for _, targetResult := range result.Targets {
					if !targetResult.OK {
						b.Fatalf("syncAllPaged target = %#v, want ok", targetResult)
					}
				}
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(float64(requests.Load())/iterations, "requests/op")
			b.ReportMetric(float64(wireBytes.Load())/iterations, "wire_B/op")
			b.ReportMetric(keyCount, "keys/op")
		})
	}
}
