package hatriecache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
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

func BenchmarkHTTPReplicatorTargetFanout(b *testing.B) {
	const targetCount = 4
	servers := make([]*httptest.Server, 0, targetCount)
	groups := make([]replicationTaskGroup, 0, targetCount)
	for idx := 0; idx < targetCount; idx++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			time.Sleep(2 * time.Millisecond)
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		}))
		servers = append(servers, server)
		groups = append(groups, replicationTaskGroup{
			target:   TopologyNode{ID: "node-" + strconv.Itoa(idx), Address: server.URL},
			payloads: []CacheCommandRequest{{Command: "INTERNALDEL", Key: "session:1"}},
		})
	}
	b.Cleanup(func() {
		for _, server := range servers {
			server.Close()
		}
	})
	for _, tt := range []struct {
		name        string
		maxInFlight int
	}{
		{name: "Serial", maxInFlight: 1},
		{name: "Bounded4", maxInFlight: 4},
	} {
		b.Run(tt.name, func(b *testing.B) {
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: servers[0].Client(), MaxInFlightTargets: tt.maxInFlight})
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				result := replicator.executeReplicationTaskGroups(context.Background(), ReplicationResult{}, groups)
				if len(result.Targets) != targetCount {
					b.Fatalf("targets = %d, want %d", len(result.Targets), targetCount)
				}
				for _, target := range result.Targets {
					if !target.OK {
						b.Fatalf("target = %#v, want ok", target)
					}
				}
			}
			b.ReportMetric(targetCount, "targets/op")
		})
	}
}

func BenchmarkReplicationRoutingPlanning(b *testing.B) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
			{ID: "node-c", Address: "http://node-c"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
			{ID: 1, Primary: "node-b", Replicas: []string{"node-a", "node-c"}},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Topology: topology, Election: election})
	keys := make([]string, 10000)
	for idx := range keys {
		keys[idx] = "session:" + strconv.Itoa(idx)
	}

	b.Run("PerKeyDynamic", func(b *testing.B) {
		b.ReportAllocs()
		for iter := 0; iter < b.N; iter++ {
			targets := 0
			for _, key := range keys {
				route, ok := replicator.routeForKey(key)
				if !ok {
					b.Fatal("routeForKey() failed")
				}
				targets += len(replicator.replicationTargets(route))
			}
			if targets == 0 {
				b.Fatal("dynamic routing returned no targets")
			}
		}
	})
	b.Run("SnapshotPerPage", func(b *testing.B) {
		b.ReportAllocs()
		for iter := 0; iter < b.N; iter++ {
			snapshot, ok := replicator.snapshotReplicationRouting()
			if !ok {
				b.Fatal("snapshotReplicationRouting() failed")
			}
			targets := 0
			for _, key := range keys {
				route, ok := snapshot.routeForKey(key)
				if !ok {
					b.Fatal("snapshot routeForKey() failed")
				}
				targets += len(snapshot.replicationTargets(route, replicator.self))
			}
			if targets == 0 {
				b.Fatal("snapshot routing returned no targets")
			}
		}
	})
}

func BenchmarkReplicationBatchMetadataWire(b *testing.B) {
	const payloadCount = 10000
	payloads := make([]CacheCommandRequest, payloadCount)
	for idx := range payloads {
		payloads[idx] = CacheCommandRequest{
			Command: "INTERNALSET",
			Key:     "session:" + strconv.Itoa(idx),
			Value:   `{"type":"string","string":"value"}`,
			Pairs: Map{
				replicationMetaSourceNode:          "node-a",
				replicationMetaSequence:            strconv.Itoa(idx + 1),
				replicationMetaTopologyFingerprint: "fingerprint-a",
			},
		}
	}

	for _, tt := range []struct {
		name  string
		build func([]CacheCommandRequest) (CacheCommandRequest, error)
	}{
		{name: "LegacyPerItem", build: replicationBatchPayload},
		{name: "SharedEnvelope", build: replicationBatchEnvelopePayload},
	} {
		b.Run(tt.name, func(b *testing.B) {
			request, err := tt.build(payloads)
			if err != nil {
				b.Fatal(err)
			}
			body, _, _, err := commandRequestBody(request, CommandWireFormatProtobuf, 0, 0)
			if err != nil {
				b.Fatal(err)
			}
			wireBytes, err := io.Copy(io.Discard, body)
			if err != nil {
				b.Fatal(err)
			}
			if closer, ok := body.(io.Closer); ok {
				_ = closer.Close()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				request, err := tt.build(payloads)
				if err != nil {
					b.Fatal(err)
				}
				body, _, _, err := commandRequestBody(request, CommandWireFormatProtobuf, 0, 0)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, body); err != nil {
					b.Fatal(err)
				}
				if closer, ok := body.(io.Closer); ok {
					if err := closer.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(wireBytes), "wire_B/op")
		})
	}
}

func BenchmarkReplicationSyncTargetPlanning(b *testing.B) {
	const payloadCount = 10000
	replicator := &HTTPReplicator{self: "node-a"}
	targets := []TopologyNode{
		{ID: "node-b", Address: "http://node-b"},
		{ID: "node-c", Address: "http://node-c"},
	}
	payloads := make([]CacheCommandRequest, payloadCount)
	for idx := range payloads {
		payloads[idx] = CacheCommandRequest{
			Command: "INTERNALSET",
			Key:     "session:" + strconv.Itoa(idx),
			Value:   `{"type":"string","string":"value"}`,
		}
	}

	b.Run("TasksThenGroup", func(b *testing.B) {
		b.ReportAllocs()
		for iter := 0; iter < b.N; iter++ {
			tasks := make([]replicationTask, 0, payloadCount*len(targets))
			for _, payload := range payloads {
				tasks = replicator.appendReplicationTasksForTargetsWithFingerprint(tasks, targets, payload, "fingerprint-a")
			}
			groups := groupReplicationTasksByTarget(tasks)
			if len(groups) != len(targets) {
				b.Fatalf("groups len = %d, want %d", len(groups), len(targets))
			}
		}
	})
	b.Run("DirectPreallocatedGroups", func(b *testing.B) {
		b.ReportAllocs()
		for iter := 0; iter < b.N; iter++ {
			groups := make([]replicationTaskGroup, 0, len(targets))
			indexes := make(map[TopologyNode]int, len(targets))
			for _, payload := range payloads {
				groups = replicator.appendReplicationPayloadToTargetGroups(groups, indexes, payloadCount, targets, payload, "fingerprint-a")
			}
			if len(groups) != len(targets) {
				b.Fatalf("groups len = %d, want %d", len(groups), len(targets))
			}
		}
	})
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

func BenchmarkSplitReplicationTaskGroupByMaxBytes(b *testing.B) {
	const payloadCount = 4096
	const maxBytes = 16 << 10
	group := replicationTaskGroup{
		target:   TopologyNode{ID: "node-b", Address: "http://127.0.0.1/node-b"},
		payloads: make([]CacheCommandRequest, 0, payloadCount),
		keys:     make([]string, 0, payloadCount),
	}
	payloadBytes := make([]int, 0, payloadCount)
	threshold := maxBytes + 1
	for idx := 0; idx < payloadCount; idx++ {
		key := "session:" + strconv.Itoa(idx)
		payload := CacheCommandRequest{
			Command: "INTERNALSET",
			Key:     key,
			Value:   `{"type":"string","string":"value-` + strconv.Itoa(idx) + `"}`,
		}
		group.payloads = append(group.payloads, payload)
		group.keys = append(group.keys, key)
		payloadBytes = append(payloadBytes, estimatedReplicationRequestBytesWithin(payload, threshold))
	}
	groupWithBytes := group
	groupWithBytes.payloadBytes = payloadBytes

	for _, tt := range []struct {
		name  string
		group replicationTaskGroup
	}{
		{name: "EstimateInSplit", group: group},
		{name: "CarriedPayloadBytes", group: groupWithBytes},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for idx := 0; idx < b.N; idx++ {
				groups := splitReplicationTaskGroupByMaxBytes(tt.group, maxBytes)
				if len(groups) == 0 {
					b.Fatal("splitReplicationTaskGroupByMaxBytes() returned no groups")
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
