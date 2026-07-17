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

func BenchmarkGroupReplicationTasksByTarget(b *testing.B) {
	for _, tt := range []struct {
		name          string
		uniqueTargets int
		tasks         int
	}{
		{name: "TwoTargetsTwoTasks", uniqueTargets: 2, tasks: 2},
		{name: "ThreeTargetsTwelveTasks", uniqueTargets: 3, tasks: 12},
		{name: "FourTargetsSixteenTasks", uniqueTargets: 4, tasks: 16},
		{name: "EightTargetsSixteenTasks", uniqueTargets: 8, tasks: 16},
		{name: "EightTargetsSixtyFourTasks", uniqueTargets: 8, tasks: 64},
		{name: "SixtyFourTargetsOneKTasks", uniqueTargets: 64, tasks: 1024},
	} {
		tasks := replicationGroupingBenchmarkTasks(tt.uniqueTargets, tt.tasks)
		b.Run(tt.name+"/Production", func(b *testing.B) {
			b.ReportAllocs()
			for idx := 0; idx < b.N; idx++ {
				groups := groupReplicationTasksByTarget(tasks)
				if len(groups) != tt.uniqueTargets {
					b.Fatalf("groups len = %d, want %d", len(groups), tt.uniqueTargets)
				}
			}
		})
		b.Run(tt.name+"/MapOnly", func(b *testing.B) {
			b.ReportAllocs()
			for idx := 0; idx < b.N; idx++ {
				groups := groupReplicationTasksByTargetMap(tasks)
				if len(groups) != tt.uniqueTargets {
					b.Fatalf("groups len = %d, want %d", len(groups), tt.uniqueTargets)
				}
			}
		})
		b.Run(tt.name+"/LinearOnly", func(b *testing.B) {
			b.ReportAllocs()
			for idx := 0; idx < b.N; idx++ {
				groups, ok := groupReplicationTasksByTargetLinear(tasks, 0)
				if !ok {
					b.Fatal("groupReplicationTasksByTargetLinear() unexpectedly hit target limit")
				}
				if len(groups) != tt.uniqueTargets {
					b.Fatalf("groups len = %d, want %d", len(groups), tt.uniqueTargets)
				}
			}
		})
	}
}

func replicationGroupingBenchmarkTasks(uniqueTargets int, taskCount int) []replicationTask {
	tasks := make([]replicationTask, 0, taskCount)
	for idx := 0; idx < taskCount; idx++ {
		targetID := "node-" + strconv.Itoa(idx%uniqueTargets)
		tasks = append(tasks, replicationTask{
			target: TopologyNode{ID: targetID, Address: "http://127.0.0.1/" + targetID},
			payload: CacheCommandRequest{
				Command: "INTERNALSET",
				Key:     "session:" + strconv.Itoa(idx),
				Value:   `{"type":"string","string":"value"}`,
			},
		})
	}
	return tasks
}
