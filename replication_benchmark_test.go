package hatriecache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"
)

var replicationOutboxBenchmarkData []byte

type benchmarkGRPCWireStats struct {
	outbound atomic.Int64
	inbound  atomic.Int64
}

func (handler *benchmarkGRPCWireStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (handler *benchmarkGRPCWireStats) HandleRPC(_ context.Context, rpcStats stats.RPCStats) {
	if payload, ok := rpcStats.(*stats.OutPayload); ok {
		handler.outbound.Add(int64(payload.WireLength))
	}
	if payload, ok := rpcStats.(*stats.InPayload); ok {
		handler.inbound.Add(int64(payload.WireLength))
	}
}

func (handler *benchmarkGRPCWireStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (handler *benchmarkGRPCWireStats) HandleConn(context.Context, stats.ConnStats) {}

type benchmarkCountingReadCloser struct {
	io.ReadCloser
	bytes *atomic.Int64
}

type benchmarkCountingResponseWriter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (writer benchmarkCountingResponseWriter) Write(data []byte) (int, error) {
	n, err := writer.ResponseWriter.Write(data)
	writer.bytes.Add(int64(n))
	return n, err
}

func (reader benchmarkCountingReadCloser) Read(data []byte) (int, error) {
	n, err := reader.ReadCloser.Read(data)
	reader.bytes.Add(int64(n))
	return n, err
}

func BenchmarkHTTPReplicatorSyncAllBatching(b *testing.B) {
	const keyCount = 10000
	for _, tt := range []struct {
		name     string
		pageSize int
	}{
		{name: "Batched10k", pageSize: keyCount},
		{name: "Default1k", pageSize: defaultReplicationSyncKeyPageSize},
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

func BenchmarkReplicationDigestChangesDefaultWire(b *testing.B) {
	const keyCount = 1024
	for _, tt := range []struct {
		name        string
		existingKey func(int) bool
	}{
		{name: "AllSet", existingKey: func(int) bool { return true }},
		{name: "HalfDelete", existingKey: func(index int) bool { return index%2 == 0 }},
	} {
		b.Run(tt.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			changes := make([]replicationDigestChange, 0, keyCount)
			for index := 0; index < keyCount; index++ {
				key := fmt.Sprintf("session:%08d", index)
				changes = append(changes, replicationDigestChange{key: key, delete: !tt.existingKey(index)})
				if tt.existingKey(index) {
					trie.UpsertString(key, "value-"+strconv.Itoa(index))
				}
			}

			targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
			}))
			b.Cleanup(targetServer.Close)
			topology := replicationTestTopology(b, targetServer.URL)
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: NewElectionStore(topology, ElectionOptions{}),
				Client:   targetServer.Client(),
			})
			b.Cleanup(replicator.Close)
			routing, ok := replicator.snapshotReplicationRouting()
			if !ok {
				b.Fatal("snapshotReplicationRouting() ok = false")
			}
			target := routing.nodes["node-b"]

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				targets, changed, deleted, _ := replicator.executeReplicationDigestChanges(
					context.Background(), trie, routing, target, changes, nil, false,
				)
				if len(targets) != 1 || !targets[0].OK || changed+deleted != keyCount {
					b.Fatalf("digest changes = %#v changed/deleted %d/%d", targets, changed, deleted)
				}
			}
		})
	}
}

func BenchmarkReplicationDigestSourceIteratorModes(b *testing.B) {
	const keyCount = 10000
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	value := strings.Repeat("v", 1024)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString(fmt.Sprintf("session:%08d", index), value)
	}
	topology := replicationTestTopology(b, "http://node-b")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
	})
	b.Cleanup(replicator.Close)
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		b.Fatal("snapshotReplicationRouting() ok = false")
	}
	inventory := replicationDigestTargetInventory{
		target:   routing.nodes["node-b"],
		prefix:   "session:",
		pageSize: defaultReplicationSyncKeyPageSize,
	}

	for _, tt := range []struct {
		name string
		new  func() *replicationDigestSourceIterator
	}{
		{
			name: "DigestValues",
			new: func() *replicationDigestSourceIterator {
				return newReplicationDigestSourceIterator(context.Background(), trie, routing, "node-a", inventory)
			},
		},
		{
			name: "KeysOnly",
			new: func() *replicationDigestSourceIterator {
				return newReplicationDigestKeySourceIterator(context.Background(), trie, routing, "node-a", inventory)
			},
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				iterator := tt.new()
				entries := 0
				for {
					_, ok, err := iterator.next()
					if err != nil {
						iterator.close()
						b.Fatal(err)
					}
					if !ok {
						break
					}
					entries++
				}
				iterator.close()
				if entries != keyCount {
					b.Fatalf("iterator entries = %d, want %d", entries, keyCount)
				}
			}
			b.ReportMetric(keyCount, "keys/op")
		})
	}
}

func BenchmarkReplicationDigestFallbackCollectionModes(b *testing.B) {
	const keyCount = 10000
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString(fmt.Sprintf("session:%08d", index), "value")
	}
	topology := replicationTestTopology(b, "http://node-b")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
	})
	b.Cleanup(replicator.Close)
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		b.Fatal("snapshotReplicationRouting() ok = false")
	}
	inventory := replicationDigestTargetInventory{
		target:   routing.nodes["node-b"],
		prefix:   "session:",
		pageSize: keyCount,
	}

	b.Run("BufferedEntries", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			source := newReplicationDigestKeySourceIterator(context.Background(), trie, routing, "node-a", inventory)
			source.entries = make([]replicationDigestSourceEntry, 0, replicationDigestInitialPageEntries)
			changes := make([]replicationDigestChange, 0, keyCount)
			for {
				entry, ok, err := source.next()
				if err != nil {
					source.close()
					b.Fatal(err)
				}
				if !ok {
					break
				}
				changes = append(changes, replicationDigestChange{key: entry.key})
			}
			source.close()
			if len(changes) != keyCount {
				b.Fatalf("buffered changes = %d, want %d", len(changes), keyCount)
			}
		}
		b.ReportMetric(keyCount, "keys/op")
	})

	b.Run("DirectChanges", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			source := newReplicationDigestKeySourceIterator(context.Background(), trie, routing, "node-a", inventory)
			changes := make([]replicationDigestChange, 0, keyCount)
			done := false
			for !done {
				var err error
				changes, done, err = source.appendFallbackChanges(changes, keyCount)
				if err != nil {
					source.close()
					b.Fatal(err)
				}
			}
			source.close()
			if len(changes) != keyCount {
				b.Fatalf("direct changes = %d, want %d", len(changes), keyCount)
			}
		}
		b.ReportMetric(keyCount, "keys/op")
	})
}

var benchmarkReplicationRouteSink uint32

func BenchmarkReplicationScanRouteModes(b *testing.B) {
	const keyCount = 10000
	topology := replicationTestTopology(b, "http://node-b")
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: NewElectionStore(topology, ElectionOptions{}),
	})
	b.Cleanup(replicator.Close)
	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		b.Fatal("snapshotReplicationRouting() ok = false")
	}
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = "session:" + strconv.Itoa(index)
	}

	for _, tt := range []struct {
		name  string
		route func(string) (ElectionKeyRoute, bool)
	}{
		{name: "Generic", route: routing.routeForKey},
		{name: "SingleShard", route: routing.replicationScanRouteForKey},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(keyCount, "routes/op")
			var checksum uint32
			for iteration := 0; iteration < b.N; iteration++ {
				for _, key := range keys {
					route, routed := tt.route(key)
					if !routed {
						b.Fatalf("route(%q) ok = false", key)
					}
					checksum += route.Route.Shard.ID + uint32(len(route.Route.Owners))
				}
			}
			benchmarkReplicationRouteSink = checksum
		})
	}
}

func BenchmarkHatTrieScanOrderModes(b *testing.B) {
	const keyCount = 10000
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString(fmt.Sprintf("session:%08d", index), "value")
	}
	for _, sorted := range []bool{false, true} {
		name := "Unordered"
		if sorted {
			name = "Ordered"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(keyCount, "keys/op")
			for iteration := 0; iteration < b.N; iteration++ {
				keys, err := trie.KeysWithPrefixChecked("session:", sorted)
				if err != nil || len(keys) != keyCount {
					b.Fatalf("KeysWithPrefixChecked(sorted=%v) = %d/%v, want %d", sorted, len(keys), err, keyCount)
				}
			}
		})
	}
}

func BenchmarkPartitionReplicationPageTraversal100k(b *testing.B) {
	keyCount := benchmarkPositiveEnvInt(b, "HATRIE_PARTITION_CURSOR_BENCH_KEYS", 100000)
	pageSize := benchmarkPositiveEnvInt(b, "HATRIE_PARTITION_CURSOR_BENCH_PAGE_SIZE", 1000)
	trie, err := CreateHatTrieWithDiskDir(b.TempDir(), false)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(trie.Destroy)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		b.Fatal(err)
	}
	for index := 0; index < keyCount; index++ {
		trie.UpsertString(fmt.Sprintf("partition-cursor:%08d", index), "value")
	}

	for _, test := range []struct {
		name   string
		packed bool
		page   func(*replicationSyncCursor, string, bool, func(Entry) error) (replicationSyncPage, error)
	}{
		{
			name: "LegacyFullMaterialize",
			page: func(_ *replicationSyncCursor, afterKey string, hasAfterKey bool, visit func(Entry) error) (replicationSyncPage, error) {
				return legacyPartitionReplicationSyncEntriesPage(trie, "partition-cursor:", afterKey, hasAfterKey, pageSize, visit)
			},
		},
		{
			name: "PersistentCursor",
			page: func(cursor *replicationSyncCursor, afterKey string, hasAfterKey bool, visit func(Entry) error) (replicationSyncPage, error) {
				return replicationSyncEntriesPageWithCursor(trie, "partition-cursor:", afterKey, hasAfterKey, pageSize, cursor, visit)
			},
		},
		{
			name:   "PackedCursor",
			packed: true,
			page: func(cursor *replicationSyncCursor, afterKey string, hasAfterKey bool, visit func(Entry) error) (replicationSyncPage, error) {
				return replicationSyncEntriesPageWithCursor(trie, "partition-cursor:", afterKey, hasAfterKey, pageSize, cursor, visit)
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				cursor := &replicationSyncCursor{packedKeys: test.packed}
				afterKey := ""
				hasAfterKey := false
				visited := 0
				pages := 0
				for {
					page, err := test.page(cursor, afterKey, hasAfterKey, func(Entry) error {
						visited++
						return nil
					})
					if err != nil {
						cursor.close(trie)
						b.Fatal(err)
					}
					pages++
					if !page.hasMore {
						break
					}
					afterKey = page.nextAfterKey
					hasAfterKey = true
				}
				cursor.close(trie)
				if visited != keyCount {
					b.Fatalf("visited %d keys, want %d", visited, keyCount)
				}
				b.ReportMetric(float64(pages), "pages/op")
			}
			b.ReportMetric(float64(keyCount), "keys/op")
		})
	}
}

func legacyPartitionReplicationSyncEntriesPage(trie *HatTrie, prefix string, afterKey string, hasAfterKey bool, limit int, visit func(Entry) error) (replicationSyncPage, error) {
	entries, err := trie.EntriesWithPrefixChecked(prefix, true)
	if err != nil {
		return replicationSyncPage{}, err
	}
	start := 0
	if hasAfterKey {
		start = sort.Search(len(entries), func(index int) bool { return entries[index].Key > afterKey })
	}
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	page := replicationSyncPage{scanned: end - start, hasMore: end < len(entries)}
	for _, entry := range entries[start:end] {
		page.nextAfterKey = entry.Key
		if visit != nil {
			if err := visit(entry); err != nil {
				return replicationSyncPage{}, err
			}
		}
	}
	return page, nil
}

func benchmarkPositiveEnvInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("invalid %s %q", name, raw)
	}
	return value
}

func BenchmarkReplicationDigestIncremental(b *testing.B) {
	const keyCount = 10000
	for _, test := range []struct {
		name          string
		changedStride int
		legacyTarget  bool
	}{
		{name: "Equal"},
		{name: "OnePercentChanged", changedStride: 100},
		{name: "LegacyFullFallback", legacyTarget: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			source := CreateHatTrie()
			targetTrie := CreateHatTrie()
			b.Cleanup(source.Destroy)
			b.Cleanup(targetTrie.Destroy)
			for idx := 0; idx < keyCount; idx++ {
				key := "session:" + strconv.Itoa(idx)
				value := replicationDigestBenchmarkValue(idx, 1)
				source.UpsertString(key, value)
				targetTrie.UpsertString(key, value)
			}

			var requests atomic.Int64
			var wireBytes atomic.Int64
			var targetHandler http.Handler
			var topology *TopologyStore
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				r.Body = benchmarkCountingReadCloser{ReadCloser: r.Body, bytes: &wireBytes}
				countingWriter := benchmarkCountingResponseWriter{ResponseWriter: w, bytes: &wireBytes}
				if !test.legacyTarget {
					targetHandler.ServeHTTP(countingWriter, r)
					return
				}
				request, format, closeBody, ok := monitoringCommandRequest(countingWriter, r)
				if !ok {
					return
				}
				defer closeBody()
				if normalizedCommand(request.Command) == replicationDigestCommand {
					writeCommandResponseWire(countingWriter, r, http.StatusOK, commandError("unsupported command"), format)
					return
				}
				response, _ := executeCacheCommand(r.Context(), targetTrie, request, commandExecutionOptions{
					NodeName:          "node-b",
					Topology:          topology,
					ReplicationSafety: NewReplicationSafetyStore(),
				})
				writeCommandResponseWire(countingWriter, r, http.StatusOK, response, format)
			}))
			b.Cleanup(target.Close)
			topology = replicationTestTopology(b, target.URL)
			targetHandler = NewMonitoringHandler(targetTrie, MonitoringOptions{
				NodeName:          "node-b",
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}).Handler()
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: NewElectionStore(topology, ElectionOptions{}),
				Client:   target.Client(),
			})
			b.Cleanup(replicator.Close)

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if test.changedStride > 0 {
					b.StopTimer()
					for idx := 0; idx < keyCount; idx += test.changedStride {
						targetTrie.UpsertString("session:"+strconv.Itoa(idx), replicationDigestBenchmarkValue(idx, 0))
					}
					b.StartTimer()
				}
				result := replicator.SyncAll(context.Background(), source, "session:")
				if result.Skipped || len(result.Targets) == 0 {
					b.Fatalf("SyncAll() = %#v, want successful digest sync", result)
				}
				wantChanged := 0
				if test.legacyTarget {
					wantChanged = keyCount
				} else if test.changedStride > 0 {
					wantChanged = keyCount / test.changedStride
				}
				if !strings.Contains(result.Reason, fmt.Sprintf("transferred %d, deleted 0", wantChanged)) {
					b.Fatalf("SyncAll() = %#v, want %d changed", result, wantChanged)
				}
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(float64(requests.Load())/iterations, "requests/op")
			b.ReportMetric(float64(wireBytes.Load())/iterations, "wire_B/op")
			b.ReportMetric(float64(wireBytes.Load())/iterations/keyCount, "wire_B/key")
		})
	}
}

func BenchmarkReplicationMerkleIndexBuild(b *testing.B) {
	const keyCount = 10000
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie := CreateHatTrie()
		for idx := 0; idx < keyCount; idx++ {
			trie.UpsertString("session:"+strconv.Itoa(idx), replicationDigestBenchmarkValue(idx, 1))
		}
		b.StartTimer()
		snapshot, err := trie.replicationMerkleSnapshot()
		b.StopTimer()
		if err != nil || snapshot.count != keyCount {
			trie.Destroy()
			b.Fatalf("replicationMerkleSnapshot() = %#v/%v, want %d entries", snapshot, err, keyCount)
		}
		retained := trie.replicationMerkleRetainedBytes()
		trie.Destroy()
		b.ReportMetric(float64(retained)/keyCount, "retained_B/key")
	}
	b.ReportAllocs()
	b.ReportMetric(keyCount, "keys/op")
}

func BenchmarkReplicationMerkleWriteTracking(b *testing.B) {
	const keyCount = 10000
	keys := make([]string, keyCount)
	for idx := range keys {
		keys[idx] = "session:" + strconv.Itoa(idx)
	}
	for _, active := range []bool{false, true} {
		name := "Inactive"
		if active {
			name = "Active"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			for idx, key := range keys {
				trie.UpsertString(key, replicationDigestBenchmarkValue(idx, 1))
			}
			if active {
				if _, err := trie.replicationMerkleSnapshot(); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(trie.replicationMerkleRetainedBytes())/keyCount, "retained_B/key")
			}
			values := [2]string{"updated-a", "updated-b"}
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				trie.UpsertString(keys[idx%len(keys)], values[idx&1])
			}
		})
	}
}

func BenchmarkReplicationMerkleChurnSnapshotCycle(b *testing.B) {
	const (
		keyCount      = 10000
		mutationCount = 100000
	)
	keys := make([]string, keyCount)
	for idx := range keys {
		keys[idx] = "session:" + strconv.Itoa(idx)
	}
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie := CreateHatTrie()
		for idx, key := range keys {
			trie.UpsertString(key, replicationDigestBenchmarkValue(idx, 1))
		}
		if _, err := trie.replicationMerkleSnapshot(); err != nil {
			trie.Destroy()
			b.Fatal(err)
		}

		b.StartTimer()
		for idx := 0; idx < mutationCount; idx++ {
			trie.UpsertString(keys[idx%keyCount], "updated")
		}
		snapshot, err := trie.replicationMerkleSnapshot()
		b.StopTimer()
		if err != nil || snapshot.count != keyCount {
			trie.Destroy()
			b.Fatalf("replicationMerkleSnapshot() = %#v/%v, want %d entries", snapshot, err, keyCount)
		}
		trie.Destroy()
	}
	b.ReportAllocs()
	b.ReportMetric(mutationCount, "writes/op")
}

func BenchmarkReplicationMerkleSnapshotAfterChurn(b *testing.B) {
	const keyCount = 10000
	keys := make([]string, keyCount)
	for idx := range keys {
		keys[idx] = "session:" + strconv.Itoa(idx)
	}
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie := CreateHatTrie()
		for idx, key := range keys {
			trie.UpsertString(key, replicationDigestBenchmarkValue(idx, 1))
		}
		if _, err := trie.replicationMerkleSnapshot(); err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		for idx := 0; idx < keyCount; idx++ {
			trie.UpsertString(keys[idx], "updated")
		}

		b.StartTimer()
		snapshot, err := trie.replicationMerkleSnapshot()
		b.StopTimer()
		if err != nil || snapshot.count != keyCount {
			trie.Destroy()
			b.Fatalf("replicationMerkleSnapshot() = %#v/%v, want %d entries", snapshot, err, keyCount)
		}
		trie.Destroy()
	}
	b.ReportAllocs()
	b.ReportMetric(keyCount, "dirty_keys/op")
}

type replicationMerklePendingBenchmarkKey struct {
	hash uint64
	key  string
}

func BenchmarkReplicationMerklePendingDedup(b *testing.B) {
	for _, cardinality := range []int{1, 4, 8, 16, 32, 64} {
		keys := make([]string, cardinality)
		hashes := make([]uint64, cardinality)
		for idx := range keys {
			keys[idx] = "session:" + strconv.Itoa(idx)
			hashes[idx] = xxhash.Sum64String(keys[idx])
		}
		b.Run("Inline"+strconv.Itoa(cardinality), func(b *testing.B) {
			pending := make([]replicationMerklePendingBenchmarkKey, cardinality)
			for idx := range pending {
				pending[idx] = replicationMerklePendingBenchmarkKey{hash: hashes[idx], key: keys[idx]}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				hash := hashes[idx%cardinality]
				for pendingIdx := range pending {
					if pending[pendingIdx].hash == hash {
						pending[pendingIdx].key = keys[idx%cardinality]
						break
					}
				}
			}
		})
		b.Run("Map"+strconv.Itoa(cardinality), func(b *testing.B) {
			pending := make(map[uint64]string, cardinality)
			for idx := range keys {
				pending[hashes[idx]] = keys[idx]
			}
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				pending[hashes[idx%cardinality]] = keys[idx%cardinality]
			}
		})
		b.Run("InlineString"+strconv.Itoa(cardinality), func(b *testing.B) {
			pending := append([]string(nil), keys...)
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				key := keys[idx%cardinality]
				for pendingIdx := range pending {
					if pending[pendingIdx] == key {
						pending[pendingIdx] = key
						break
					}
				}
			}
		})
		b.Run("MapString"+strconv.Itoa(cardinality), func(b *testing.B) {
			pending := make(map[string]struct{}, cardinality)
			for idx := range keys {
				pending[keys[idx]] = struct{}{}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				pending[keys[idx%cardinality]] = struct{}{}
			}
		})
	}
}

func BenchmarkReplicationMerkleIncremental(b *testing.B) {
	const keyCount = 10000
	for _, test := range []struct {
		name          string
		changedStride int
	}{
		{name: "Equal"},
		{name: "OnePercentChanged", changedStride: 100},
	} {
		b.Run(test.name, func(b *testing.B) {
			source := CreateHatTrie()
			targetTrie := CreateHatTrie()
			b.Cleanup(source.Destroy)
			b.Cleanup(targetTrie.Destroy)
			for idx := 0; idx < keyCount; idx++ {
				key := "session:" + strconv.Itoa(idx)
				value := replicationDigestBenchmarkValue(idx, 1)
				source.UpsertString(key, value)
				targetTrie.UpsertString(key, value)
			}
			if _, err := source.replicationMerkleSnapshot(); err != nil {
				b.Fatal(err)
			}
			if _, err := targetTrie.replicationMerkleSnapshot(); err != nil {
				b.Fatal(err)
			}

			var requests atomic.Int64
			var wireBytes atomic.Int64
			var targetHandler http.Handler
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				r.Body = benchmarkCountingReadCloser{ReadCloser: r.Body, bytes: &wireBytes}
				targetHandler.ServeHTTP(benchmarkCountingResponseWriter{ResponseWriter: w, bytes: &wireBytes}, r)
			}))
			b.Cleanup(target.Close)
			topology := replicationTestTopology(b, target.URL)
			targetHandler = NewMonitoringHandler(targetTrie, MonitoringOptions{
				NodeName:          "node-b",
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}).Handler()
			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:     "node-a",
				Topology: topology,
				Election: NewElectionStore(topology, ElectionOptions{}),
				Client:   target.Client(),
			})
			b.Cleanup(replicator.Close)

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if test.changedStride > 0 {
					b.StopTimer()
					for idx := 0; idx < keyCount; idx += test.changedStride {
						targetTrie.UpsertString("session:"+strconv.Itoa(idx), replicationDigestBenchmarkValue(idx, 0))
					}
					b.StartTimer()
				}
				result := replicator.SyncAll(context.Background(), source, "")
				if result.Skipped || len(result.Targets) == 0 {
					b.Fatalf("SyncAll() = %#v, want successful Merkle sync", result)
				}
				wantChanged := 0
				if test.changedStride > 0 {
					wantChanged = keyCount / test.changedStride
				}
				if !strings.Contains(result.Reason, fmt.Sprintf("transferred %d, deleted 0", wantChanged)) && !(wantChanged == 0 && strings.Contains(result.Reason, "merkle equal")) {
					b.Fatalf("SyncAll() = %#v, want %d changed", result, wantChanged)
				}
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(float64(requests.Load())/iterations, "requests/op")
			b.ReportMetric(float64(wireBytes.Load())/iterations, "wire_B/op")
			b.ReportMetric(float64(source.replicationMerkleRetainedBytes())/keyCount, "retained_B/key")
		})
	}
}

func BenchmarkReplicationOutboxEncoding(b *testing.B) {
	record := newReplicationOutboxJob(replicationOutboxBenchmarkJob(1, 4096))
	for _, codec := range []ReplicationOutboxCodec{ReplicationOutboxCodecJSON, ReplicationOutboxCodecBinary} {
		b.Run(string(codec), func(b *testing.B) {
			var data []byte
			var err error
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if codec == ReplicationOutboxCodecJSON {
					data, err = json.Marshal(record)
				} else {
					data, err = marshalReplicationOutboxJobBinary(record)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			replicationOutboxBenchmarkData = data
			b.ReportMetric(float64(len(data)), "stored_B/op")
		})
	}
}

func BenchmarkReplicationOutboxDurableEnqueue(b *testing.B) {
	const writers = 32
	for _, test := range []struct {
		name        string
		codec       ReplicationOutboxCodec
		batchWindow time.Duration
	}{
		{name: "JSONSyncEach", codec: ReplicationOutboxCodecJSON},
		{name: "BinarySyncEach", codec: ReplicationOutboxCodecBinary},
		{name: "BinaryGroupCommit", codec: ReplicationOutboxCodecBinary, batchWindow: DefaultReplicationOutboxBatchWindow},
	} {
		b.Run(test.name, func(b *testing.B) {
			store, err := OpenLevelDBReplicationOutboxWithOptions(b.TempDir(), ReplicationOutboxOptions{
				Codec:       test.codec,
				BatchWindow: test.batchWindow,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				start := make(chan struct{})
				errs := make(chan error, writers)
				var group sync.WaitGroup
				group.Add(writers)
				for writer := 0; writer < writers; writer++ {
					id := uint64(iteration*writers + writer + 1)
					go func() {
						defer group.Done()
						<-start
						errs <- store.putJob(replicationOutboxBenchmarkJob(id, 1024))
					}()
				}
				close(start)
				group.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(writers, "jobs/op")
			b.ReportMetric(float64(store.levelDBSyncWriteCount())/float64(b.N), "sync_writes/op")
		})
	}
}

func BenchmarkJournalBackedReplicationOutbox(b *testing.B) {
	for _, journalBacked := range []bool{false, true} {
		name := "LegacyFullOutbox"
		if journalBacked {
			name = "JournalReference"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			journal, err := OpenCommandJournalWithOptions(filepath.Join(dir, "commands.journal"), CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
			outbox, err := OpenLevelDBReplicationOutboxWithOptions(filepath.Join(dir, "outbox"), ReplicationOutboxOptions{
				Codec: ReplicationOutboxCodecBinary,
			})
			if err != nil {
				b.Fatal(err)
			}
			if journalBacked {
				if err := outbox.AttachJournal(journal); err != nil {
					b.Fatal(err)
				}
			}
			request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark:key", Value: strings.Repeat("payload-", 512)}
			jobTemplate := replicationOutboxBenchmarkJob(1, 4096)
			fullRecord := newReplicationOutboxJob(jobTemplate)
			fullBytes, err := marshalReplicationOutboxJobBinary(fullRecord)
			if err != nil {
				b.Fatal(err)
			}
			journalEntry := commandJournalEntry{Version: commandJournalVersion, Sequence: 1, Request: request}
			if journalBacked {
				journalRecord := fullRecord
				journalRecord.JournalSequence = 1
				journalEntry.Outbox = &journalRecord
			}
			journalBytes, err := marshalCommandJournalEntry(journalEntry, CommandJournalFormatBinary)
			if err != nil {
				b.Fatal(err)
			}
			encodedBytes := len(journalBytes) + len(fullBytes)
			if journalBacked {
				referenceBytes, err := marshalReplicationOutboxJobBinary(replicationOutboxJob{ID: 1, JournalSequence: 1})
				if err != nil {
					b.Fatal(err)
				}
				encodedBytes = len(journalBytes) + len(referenceBytes)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				job := jobTemplate
				job.id = uint64(iteration + 1)
				journal.mu.Lock()
				if journalBacked {
					record := newReplicationOutboxJob(job)
					_, sequence, err := journal.writeWithOutboxWithoutSyncLocked(request, &record)
					job.journalSeq = sequence
					if err == nil {
						err = journal.syncLocked()
					}
					journal.mu.Unlock()
					if err != nil {
						b.Fatal(err)
					}
				} else {
					_, err := journal.appendLocked(request)
					journal.mu.Unlock()
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := outbox.putJob(job); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := outbox.Close(); err != nil {
				b.Fatal(err)
			}
			if err := journal.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(encodedBytes), "encoded_B/op")
			b.ReportMetric(float64(b.N+int(outbox.levelDBSyncWriteCount()))/float64(b.N), "sync_writes/op")
			b.ReportMetric(float64(replicationBenchmarkDirectoryBytes(b, dir))/float64(b.N), "disk_B/op")
		})
	}
}

func replicationBenchmarkDirectoryBytes(b *testing.B, path string) int64 {
	b.Helper()
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return total
}

func BenchmarkReplicationOutboxReplay10k(b *testing.B) {
	const jobs = 10000
	for _, codec := range []ReplicationOutboxCodec{ReplicationOutboxCodecJSON, ReplicationOutboxCodecBinary} {
		b.Run(string(codec), func(b *testing.B) {
			store, err := OpenLevelDBReplicationOutboxWithOptions(b.TempDir(), ReplicationOutboxOptions{Codec: codec})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			batch := new(leveldb.Batch)
			storedBytes := 0
			for id := 1; id <= jobs; id++ {
				record := newReplicationOutboxJob(replicationOutboxBenchmarkJob(uint64(id), 1024))
				data, err := store.marshalJob(record)
				if err != nil {
					b.Fatal(err)
				}
				storedBytes += len(data)
				batch.Put(replicationOutboxLevelDBJobKey(uint64(id)), data)
			}
			if err := store.db.Write(batch, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if restored := store.jobs(); len(restored) != jobs {
					b.Fatalf("jobs() = %d, want %d", len(restored), jobs)
				}
			}
			b.StopTimer()
			b.ReportMetric(jobs, "jobs/op")
			b.ReportMetric(float64(storedBytes)/jobs, "stored_B/job")
		})
	}
}

func BenchmarkReplicationOutboxRestore100k(b *testing.B) {
	const jobs = 100000
	store, err := OpenLevelDBReplicationOutboxWithOptions(b.TempDir(), ReplicationOutboxOptions{
		Codec:       ReplicationOutboxCodecBinary,
		BatchWindow: 0,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	batch := new(leveldb.Batch)
	for id := 1; id <= jobs; id++ {
		data, err := store.marshalJob(newReplicationOutboxJob(replicationOutboxBenchmarkJob(uint64(id), 64)))
		if err != nil {
			b.Fatal(err)
		}
		batch.Put(replicationOutboxLevelDBJobKey(uint64(id)), data)
	}
	if err := store.db.Write(batch, nil); err != nil {
		b.Fatal(err)
	}
	batch.Reset()

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		replicator := NewHTTPReplicator(HTTPReplicatorOptions{
			Context:        ctx,
			AsyncQueueSize: 1024,
			AsyncOutbox:    store,
		})
		b.ReportMetric(float64(cap(replicator.queue)), "resident_jobs/op")
		b.StopTimer()
		replicator.Close()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(jobs, "durable_jobs/op")
}

func replicationOutboxBenchmarkJob(id uint64, valueBytes int) replicationJob {
	return replicationJob{
		id:     id,
		result: ReplicationResult{Command: "SETBYTES", Key: fmt.Sprintf("session:%08d", id), Queued: true},
		tasks: []replicationTask{{
			target: TopologyNode{ID: "node-b", Address: "http://node-b:8080", GRPCAddress: "node-b:9090"},
			payload: CacheCommandRequest{
				Command:     replicationSetCompactCommand,
				Key:         fmt.Sprintf("session:%08d", id),
				BinaryValue: replicationOutboxBenchmarkValue(id, valueBytes),
				Pairs: Map{
					replicationMetaSourceNode:          "node-a",
					replicationMetaSequence:            strconv.FormatUint(id, 10),
					replicationMetaTopologyFingerprint: "benchmark-topology",
				},
			},
		}},
		enqueuedAt: time.Unix(1700000000, int64(id)).UTC(),
	}
}

func replicationOutboxBenchmarkValue(seed uint64, size int) []byte {
	value := make([]byte, size)
	state := seed*0x9e3779b97f4a7c15 + 1
	for index := range value {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		value[index] = byte(state)
	}
	return value
}

func replicationDigestBenchmarkValue(key int, version int) string {
	data := make([]byte, 1024)
	state := uint64(key+1)*0x9e3779b97f4a7c15 + uint64(version+1)
	for idx := range data {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		data[idx] = byte(state)
	}
	return string(data)
}

func BenchmarkReplicationSyncTransport(b *testing.B) {
	const keyCount = 10000
	for _, transport := range []ReplicationTransport{ReplicationTransportHTTP, ReplicationTransportGRPCStream} {
		b.Run(string(transport), func(b *testing.B) {
			sourceTrie := CreateHatTrie()
			targetTrie := CreateHatTrie()
			b.Cleanup(sourceTrie.Destroy)
			b.Cleanup(targetTrie.Destroy)
			for idx := 0; idx < keyCount; idx++ {
				sourceTrie.UpsertString("session:"+strconv.Itoa(idx), "value-"+strconv.Itoa(idx))
			}

			httpListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatalf("HTTP listen: %v", err)
			}
			grpcListener := bufconn.Listen(testGRPCBufferSize)
			topology, err := NewTopologyStore(ClusterTopology{
				Version: 1,
				Self:    "node-a",
				Nodes: []TopologyNode{
					{ID: "node-a", Address: "http://node-a"},
					{ID: "node-b", Address: "http://" + httpListener.Addr().String(), GRPCAddress: "bufnet"},
				},
				Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
			})
			if err != nil {
				b.Fatalf("NewTopologyStore() error = %v", err)
			}

			var httpRequests atomic.Int64
			var httpWireBytes atomic.Int64
			monitoring := NewMonitoringHandler(targetTrie, MonitoringOptions{
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}).Handler()
			httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				r.Body = benchmarkCountingReadCloser{ReadCloser: r.Body, bytes: &httpWireBytes}
				monitoring.ServeHTTP(w, r)
			})}
			go func() { _ = httpServer.Serve(httpListener) }()
			b.Cleanup(func() {
				_ = httpServer.Close()
				_ = httpListener.Close()
			})

			grpcServer := grpc.NewServer()
			RegisterCacheGRPCServer(grpcServer, NewCacheGRPCServer(targetTrie, CacheGRPCOptions{
				NodeName:          "node-b",
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}))
			go func() { _ = grpcServer.Serve(grpcListener) }()
			b.Cleanup(func() {
				grpcServer.Stop()
				_ = grpcListener.Close()
			})

			grpcWireStats := &benchmarkGRPCWireStats{}
			options := HTTPReplicatorOptions{
				Self:      "node-a",
				Topology:  topology,
				Election:  NewElectionStore(topology, ElectionOptions{}),
				Transport: transport,
			}
			if transport == ReplicationTransportGRPCStream {
				options.DisableHTTPFallback = true
				options.GRPCDialOptions = []grpc.DialOption{
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
						return grpcListener.Dial()
					}),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithStatsHandler(grpcWireStats),
				}
			}
			replicator := NewHTTPReplicator(options)
			b.Cleanup(replicator.Close)

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				result := replicator.syncAllPaged(context.Background(), sourceTrie, "session:", defaultReplicationSyncKeyPageSize)
				if result.Skipped || result.Entries != keyCount {
					b.Fatalf("syncAllPaged() = %#v, want %d entries", result, keyCount)
				}
				for _, targetResult := range result.Targets {
					if !targetResult.OK {
						b.Fatalf("sync target = %#v, want ok", targetResult)
					}
				}
			}
			b.StopTimer()
			iterations := float64(b.N)
			b.ReportMetric(keyCount, "keys/op")
			if transport == ReplicationTransportGRPCStream {
				b.ReportMetric(float64(replicator.grpcStreamBatches.Load())/iterations, "batches/op")
				b.ReportMetric(float64(grpcWireStats.outbound.Load())/iterations, "wire_B/op")
			} else {
				b.ReportMetric(float64(httpRequests.Load())/iterations, "batches/op")
				b.ReportMetric(float64(httpWireBytes.Load())/iterations, "wire_B/op")
			}
		})
	}
}

func BenchmarkReplicationLiveTransport10K(b *testing.B) {
	const operations = 10_000
	callers := 32
	if value := os.Getenv("HATRIE_BENCH_GRPC_LIVE_CALLERS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			b.Fatalf("HATRIE_BENCH_GRPC_LIVE_CALLERS=%q must be a positive integer", value)
		}
		callers = parsed
	}
	batchMaxCommands := 0
	if value := os.Getenv("HATRIE_BENCH_GRPC_LIVE_BATCH_MAX_COMMANDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			b.Fatalf("HATRIE_BENCH_GRPC_LIVE_BATCH_MAX_COMMANDS=%q must be a positive integer", value)
		}
		batchMaxCommands = parsed
	}
	batchWindow := time.Duration(0)
	if value := os.Getenv("HATRIE_BENCH_GRPC_LIVE_BATCH_WINDOW"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 0 {
			b.Fatalf("HATRIE_BENCH_GRPC_LIVE_BATCH_WINDOW=%q must be a non-negative duration", value)
		}
		batchWindow = parsed
	}
	for _, transport := range []ReplicationTransport{ReplicationTransportHTTP, ReplicationTransportGRPCStream} {
		b.Run(string(transport), func(b *testing.B) {
			sourceTrie := CreateHatTrie()
			targetTrie := CreateHatTrie()
			b.Cleanup(sourceTrie.Destroy)
			b.Cleanup(targetTrie.Destroy)
			httpListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatalf("HTTP listen: %v", err)
			}
			grpcListener := bufconn.Listen(testGRPCBufferSize)
			topology, err := NewTopologyStore(ClusterTopology{
				Version: 1,
				Self:    "node-a",
				Nodes: []TopologyNode{
					{ID: "node-a", Address: "http://node-a"},
					{ID: "node-b", Address: "http://" + httpListener.Addr().String(), GRPCAddress: "bufnet"},
				},
				Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
			})
			if err != nil {
				b.Fatalf("NewTopologyStore() error = %v", err)
			}

			var httpRequests atomic.Int64
			var httpWireBytes atomic.Int64
			monitoring := NewMonitoringHandler(targetTrie, MonitoringOptions{
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}).Handler()
			httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				r.Body = benchmarkCountingReadCloser{ReadCloser: r.Body, bytes: &httpWireBytes}
				monitoring.ServeHTTP(w, r)
			})}
			go func() { _ = httpServer.Serve(httpListener) }()
			b.Cleanup(func() {
				_ = httpServer.Close()
				_ = httpListener.Close()
			})

			grpcServer := grpc.NewServer()
			RegisterCacheGRPCServer(grpcServer, NewCacheGRPCServer(targetTrie, CacheGRPCOptions{
				NodeName:          "node-b",
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}))
			go func() { _ = grpcServer.Serve(grpcListener) }()
			b.Cleanup(func() {
				grpcServer.Stop()
				_ = grpcListener.Close()
			})

			grpcWireStats := &benchmarkGRPCWireStats{}
			options := HTTPReplicatorOptions{
				Self:      "node-a",
				Topology:  topology,
				Election:  NewElectionStore(topology, ElectionOptions{}),
				Transport: transport,
			}
			if transport == ReplicationTransportGRPCStream {
				options.DisableHTTPFallback = true
				options.GRPCStreamWindow = callers
				options.GRPCLiveBatchMaxCommands = batchMaxCommands
				options.GRPCLiveBatchWindow = batchWindow
				options.GRPCDialOptions = []grpc.DialOption{
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return grpcListener.Dial() }),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithStatsHandler(grpcWireStats),
				}
			}
			replicator := NewHTTPReplicator(options)
			b.Cleanup(replicator.Close)

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var next atomic.Int64
				var failed atomic.Bool
				var workers sync.WaitGroup
				workers.Add(callers)
				for worker := 0; worker < callers; worker++ {
					go func() {
						defer workers.Done()
						for {
							idx := int(next.Add(1)) - 1
							if idx >= operations {
								return
							}
							key := fmt.Sprintf("live:%d:%05d", iteration, idx)
							sourceTrie.UpsertString(key, "replicated-value")
							result := replicator.ReplicateCommand(context.Background(), sourceTrie,
								CacheCommandRequest{Command: "SETSTR", Key: key, Value: "replicated-value"}, CacheCommandResponse{OK: true})
							if len(result.Targets) != 1 || !result.Targets[0].OK {
								failed.Store(true)
								return
							}
						}
					}()
				}
				workers.Wait()
				if failed.Load() {
					b.Fatal("live replication failed")
				}
			}
			b.StopTimer()
			if got, want := len(targetTrie.EntriesWithPrefix("live:", false)), operations*b.N; got != want {
				b.Fatalf("target entries = %d, want all %d live writes", got, want)
			}
			iterations := float64(b.N)
			b.ReportMetric(operations, "commands/op")
			b.ReportMetric(float64(callers), "callers/op")
			if transport == ReplicationTransportGRPCStream {
				b.ReportMetric(float64(replicator.grpcStreamBatches.Load())/iterations, "batches/op")
				b.ReportMetric(float64(grpcWireStats.outbound.Load())/iterations, "wire_B/op")
			} else {
				b.ReportMetric(float64(httpRequests.Load())/iterations, "batches/op")
				b.ReportMetric(float64(httpWireBytes.Load())/iterations, "wire_B/op")
			}
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
