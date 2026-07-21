package hatriecache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"google.golang.org/grpc"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const bigWinsDurableWrites = 100

func BenchmarkBigWins(b *testing.B) {
	b.Run("GlobalTelemetry", benchmarkBigWinsGlobalTelemetry)
	b.Run("ConcurrentRead", benchmarkBigWinsConcurrentRead)
	b.Run("ConcurrentWrite", benchmarkBigWinsConcurrentWrite)
	b.Run("LocalPartitions", benchmarkBigWinsLocalPartitions)
	b.Run("PerKeyMemory", func(b *testing.B) { benchmarkBigWinsPerKeyMemory(b, KeyStatsModeBounded) })
	b.Run("PerKeyMemoryFull", func(b *testing.B) { benchmarkBigWinsPerKeyMemory(b, KeyStatsModeFull) })
	b.Run("PerKeyMemoryOff", func(b *testing.B) { benchmarkBigWinsPerKeyMemory(b, KeyStatsModeOff) })
	b.Run("DurableWrite/Serial", func(b *testing.B) { benchmarkBigWinsDurableWrite(b, false) })
	b.Run("DurableWrite/Concurrent", func(b *testing.B) { benchmarkBigWinsDurableWrite(b, true) })
	b.Run("Snapshot", benchmarkBigWinsSnapshot)
	b.Run("AntiEntropy", benchmarkBigWinsAntiEntropy)
	b.Run("UnaryCommand", benchmarkBigWinsUnaryCommand)
	b.Run("StreamCommand", benchmarkBigWinsStreamCommand)
	b.Run("PipelinedStreamCommand", benchmarkBigWinsPipelinedStreamCommand)
	b.Run("NativeBatchStreamCommand", benchmarkBigWinsNativeBatchStreamCommand)
	b.Run("ScalarBatchStreamCommand", benchmarkBigWinsScalarBatchStreamCommand)
	b.Run("NativeStructuredBatchStreamCommand", benchmarkBigWinsNativeStructuredBatchStreamCommand)
	b.Run("StructuredBatchStreamCommand", benchmarkBigWinsStructuredBatchStreamCommand)
	b.Run("ChurnRetentionBaseline", benchmarkBigWinsChurnRetentionBaseline)
	b.Run("ChurnRetentionCompacted", benchmarkBigWinsChurnRetentionCompacted)
	b.Run("ExpirationDeadlineUpdate", benchmarkBigWinsExpirationDeadlineUpdate)
}

func benchmarkBigWinsExpirationDeadlineUpdate(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	now := time.Unix(1700000000, 0)
	trie.now = func() time.Time { return now }
	trie.UpsertString("ttl:hot", "value")
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if !trie.ExpireAt("ttl:hot", now.Add(time.Duration(iteration+1)*time.Second)) {
			b.Fatal("ExpireAt() = false")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(trie.expirations.Len()), "heap_entries")
}

func benchmarkBigWinsChurnRetentionBaseline(b *testing.B) {
	benchmarkBigWinsChurnRetention(b, false)
}

func benchmarkBigWinsChurnRetentionCompacted(b *testing.B) {
	benchmarkBigWinsChurnRetention(b, true)
}

func benchmarkBigWinsChurnRetention(b *testing.B, compact bool) {
	keyCount := bigWinsBenchmarkKeys(100000)
	survivorCount := (keyCount + 9) / 10
	var retainedHeap uint64
	var retainedBacking uint64
	var compactionTime time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		trie := CreateHatTrie()
		if _, err := trie.replicationMerkleSnapshot(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for idx := 0; idx < keyCount; idx++ {
			trie.UpsertString(bigWinsKey(idx), "value")
		}
		for idx := 0; idx < keyCount; idx++ {
			if idx%10 != 0 {
				trie.Delete(bigWinsKey(idx))
			}
		}
		if compact {
			started := time.Now()
			if _, err := trie.CompactMemory(); err != nil {
				b.Fatal(err)
			}
			compactionTime += time.Since(started)
		}
		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			retainedHeap += after.HeapAlloc - before.HeapAlloc
		}
		trie.mu.RLock()
		retainedBacking += trie.memoryBackingBytesLocked()
		trie.mu.RUnlock()
		if trie.Size() != survivorCount {
			b.Fatalf("churn trie size = %d, want %d", trie.Size(), survivorCount)
		}
		runtime.KeepAlive(trie)
		trie.Destroy()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(keyCount), "inserted_keys/op")
	b.ReportMetric(float64(keyCount-survivorCount), "deleted_keys/op")
	b.ReportMetric(float64(retainedHeap)/float64(b.N), "retained_heap_B/op")
	b.ReportMetric(float64(retainedBacking)/float64(b.N), "retained_backing_B/op")
	if compact {
		b.ReportMetric(float64(compactionTime.Nanoseconds())/float64(b.N), "compaction_ns/op")
	}
}

func BenchmarkMemoryCompactionReadPause10k(b *testing.B) {
	const (
		insertedKeys = 100000
		survivorStep = 10
	)
	var totalCompaction time.Duration
	var totalCutoverNanos int64
	var totalReplayedKeys int
	var maximumCatchUpRounds int
	var maximumReadPause atomic.Int64
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie := CreateHatTrie()
		for idx := 0; idx < insertedKeys; idx++ {
			trie.UpsertString(bigWinsKey(idx), "value")
		}
		for idx := 0; idx < insertedKeys; idx++ {
			if idx%survivorStep != 0 {
				trie.Delete(bigWinsKey(idx))
			}
		}
		runtime.GC()

		stop := make(chan struct{})
		ready := make(chan struct{})
		var reader sync.WaitGroup
		reader.Add(1)
		go func() {
			defer reader.Done()
			first := true
			for {
				select {
				case <-stop:
					return
				default:
				}
				started := time.Now()
				if value := trie.GetString(bigWinsKey(0)); value != "value" {
					b.Errorf("GetString() during compaction = %q, want value", value)
					return
				}
				pause := time.Since(started).Nanoseconds()
				for previous := maximumReadPause.Load(); pause > previous && !maximumReadPause.CompareAndSwap(previous, pause); previous = maximumReadPause.Load() {
				}
				if first {
					close(ready)
					first = false
				}
			}
		}()
		<-ready

		b.StartTimer()
		started := time.Now()
		result, err := trie.CompactMemory()
		totalCompaction += time.Since(started)
		totalCutoverNanos += result.CutoverNanos
		totalReplayedKeys += result.ReplayedKeys
		if result.CatchUpRounds > maximumCatchUpRounds {
			maximumCatchUpRounds = result.CatchUpRounds
		}
		b.StopTimer()
		close(stop)
		reader.Wait()
		if err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		if result.Entries != insertedKeys/survivorStep {
			trie.Destroy()
			b.Fatalf("CompactMemory().Entries = %d, want %d", result.Entries, insertedKeys/survivorStep)
		}
		trie.Destroy()
	}
	b.ReportAllocs()
	b.ReportMetric(float64(maximumCatchUpRounds), "catch_up_rounds")
	b.ReportMetric(float64(totalCompaction.Nanoseconds())/float64(b.N), "compaction_ns/op")
	b.ReportMetric(float64(totalCutoverNanos)/float64(b.N), "cutover_ns/op")
	b.ReportMetric(float64(maximumReadPause.Load()), "max_read_pause_ns")
	b.ReportMetric(float64(totalReplayedKeys)/float64(b.N), "replayed_keys/op")
}

func benchmarkBigWinsGlobalTelemetry(b *testing.B) {
	operations := bigWinsBenchmarkOperations(100000)
	for _, mode := range []KeyStatsMode{KeyStatsModeOff, KeyStatsModeFull} {
		for _, workers := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("%s/Workers%d", mode, workers), func(b *testing.B) {
				trie := CreateHatTrie()
				b.Cleanup(trie.Destroy)
				if err := trie.ConfigureKeyStats(mode, 0); err != nil {
					b.Fatal(err)
				}
				trie.UpsertString("telemetry-key", "value")

				var total time.Duration
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					started := time.Now()
					var group sync.WaitGroup
					group.Add(workers)
					for worker := 0; worker < workers; worker++ {
						go func(worker int) {
							defer group.Done()
							for operation := worker; operation < operations; operation += workers {
								if got := trie.GetString("telemetry-key"); got != "value" {
									b.Errorf("GetString() = %q, want value", got)
									return
								}
							}
						}(worker)
					}
					group.Wait()
					total += time.Since(started)
				}
				b.StopTimer()
				b.ReportMetric(float64(operations), "reads/op")
				b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/read")
			})
		}
	}
}

func benchmarkBigWinsConcurrentWrite(b *testing.B) {
	keyCount := bigWinsBenchmarkKeys(65536)
	operations := bigWinsBenchmarkOperations(100000)
	for _, config := range []struct {
		name    string
		stripes int
	}{
		{name: "Off"},
		{name: "Stripes64", stripes: 64},
	} {
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("%s/Workers%d", config.name, workers), func(b *testing.B) {
				trie := CreateHatTrie()
				b.Cleanup(trie.Destroy)
				keys := make([]string, keyCount)
				for index := range keys {
					keys[index] = bigWinsKey(index)
					trie.UpsertCounter(keys[index], 0)
				}
				if err := trie.ConfigureCounterWriteStripes(config.stripes); err != nil {
					b.Fatal(err)
				}
				var total time.Duration
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					started := time.Now()
					var group sync.WaitGroup
					group.Add(workers)
					for worker := 0; worker < workers; worker++ {
						go func(worker int) {
							defer group.Done()
							for operation := worker; operation < operations; operation += workers {
								trie.UpsertCounter(keys[operation%keyCount], int32(iteration+operation))
							}
						}(worker)
					}
					group.Wait()
					total += time.Since(started)
				}
				b.StopTimer()
				b.ReportMetric(float64(operations), "writes/op")
				b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/write")
				b.ReportMetric(float64(config.stripes)*float64(unsafe.Sizeof(sync.RWMutex{})), "stripe-B")
			})
		}
	}
}

func benchmarkBigWinsLocalPartitions(b *testing.B) {
	keyCount := bigWinsBenchmarkKeys(65536)
	operations := bigWinsBenchmarkOperations(100000)
	const workers = 16
	for _, partitions := range []int{0, 16} {
		name := "Off"
		if partitions != 0 {
			name = fmt.Sprintf("Partitions%d", partitions)
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			if err := trie.ConfigureLocalPartitions(partitions); err != nil {
				b.Fatal(err)
			}
			keys := make([]string, keyCount)
			for index := range keys {
				keys[index] = bigWinsKey(index)
				trie.UpsertCounter(keys[index], 0)
			}
			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				started := time.Now()
				var group sync.WaitGroup
				group.Add(workers)
				for worker := 0; worker < workers; worker++ {
					go func(worker int) {
						defer group.Done()
						for operation := worker; operation < operations; operation += workers {
							trie.UpsertCounter(keys[operation%keyCount], int32(iteration+operation))
						}
					}(worker)
				}
				group.Wait()
				total += time.Since(started)
			}
			b.StopTimer()
			b.ReportMetric(float64(operations), "writes/op")
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/write")
			b.ReportMetric(float64(partitions), "partitions")
		})
	}
}

func benchmarkBigWinsConcurrentRead(b *testing.B) {
	keyCount := bigWinsBenchmarkKeys(4096)
	operations := bigWinsBenchmarkOperations(100000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for idx := 0; idx < keyCount; idx++ {
		trie.UpsertString(bigWinsKey(idx), "value")
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > operations {
		workers = operations
	}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		var wg sync.WaitGroup
		wg.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func(worker int) {
				defer wg.Done()
				for operation := worker; operation < operations; operation += workers {
					if got := trie.GetString(bigWinsKey(operation % keyCount)); got != "value" {
						b.Errorf("GetString() = %q, want value", got)
						return
					}
				}
			}(worker)
		}
		wg.Wait()
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "reads/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/read")
}

func benchmarkBigWinsPerKeyMemory(b *testing.B, mode KeyStatsMode) {
	keyCount := bigWinsBenchmarkKeys(100000)
	var retained uint64
	var tracked uint64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		trie := CreateHatTrie()
		capacity := DefaultKeyStatsCapacity
		if mode != KeyStatsModeBounded {
			capacity = 0
		}
		if err := trie.ConfigureKeyStats(mode, capacity); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for idx := 0; idx < keyCount; idx++ {
			trie.UpsertString(bigWinsKey(idx), "v")
		}
		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			retained += after.HeapAlloc - before.HeapAlloc
		}
		trie.mu.Lock()
		tracked += uint64(len(trie.keyStats))
		trie.mu.Unlock()
		runtime.KeepAlive(trie)
		trie.Destroy()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(keyCount), "keys/op")
	b.ReportMetric(float64(retained)/float64(b.N*keyCount), "retained_B/key")
	b.ReportMetric(float64(tracked)/float64(b.N), "tracked_keys/op")
}

func benchmarkBigWinsDurableWrite(b *testing.B, concurrent bool) {
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie := CreateHatTrie()
		journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), fmt.Sprintf("journal-%d", iteration)))
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		started := time.Now()
		if concurrent {
			benchmarkBigWinsConcurrentJournalWrites(b, trie, journal)
		} else {
			for idx := 0; idx < bigWinsDurableWrites; idx++ {
				response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: bigWinsKey(idx), Value: "value"})
				if !response.OK {
					b.Fatalf("ExecuteCommand() = %#v", response)
				}
			}
		}
		total += time.Since(started)
		b.StopTimer()
		if trie.Size() != bigWinsDurableWrites {
			b.Fatalf("durable trie size = %d, want %d", trie.Size(), bigWinsDurableWrites)
		}
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
		trie.Destroy()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(bigWinsDurableWrites, "writes/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*bigWinsDurableWrites), "ns/durable_write")
}

func benchmarkBigWinsConcurrentJournalWrites(b *testing.B, trie *HatTrie, journal *CommandJournal) {
	const workers = 16
	var next atomic.Int64
	errors := make(chan string, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for {
				idx := int(next.Add(1) - 1)
				if idx >= bigWinsDurableWrites {
					return
				}
				response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: bigWinsKey(idx), Value: "value"})
				if !response.OK {
					errors <- response.Message
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for message := range errors {
		b.Fatalf("concurrent journal write failed: %s", message)
	}
}

func benchmarkBigWinsSnapshot(b *testing.B) {
	keyCount := bigWinsBenchmarkKeys(25000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	partitions := bigWinsBenchmarkSnapshotPartitions()
	if err := trie.ConfigureLocalPartitions(partitions); err != nil {
		b.Fatal(err)
	}
	for idx := 0; idx < keyCount; idx++ {
		trie.UpsertString(bigWinsKey(idx), "snapshot-value")
	}
	probeKey := bigWinsKey(0)
	var total time.Duration
	var maxPause atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		stop := make(chan struct{})
		ready := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			close(ready)
			for {
				select {
				case <-stop:
					return
				default:
				}
				started := time.Now()
				_ = trie.GetString(probeKey)
				updateAtomicMax(&maxPause, time.Since(started).Nanoseconds())
			}
		}()
		<-ready
		started := time.Now()
		path := filepath.Join(b.TempDir(), fmt.Sprintf("snapshot-%d.hc", iteration))
		if err := trie.SaveSnapshot(path); err != nil {
			close(stop)
			<-done
			b.Fatal(err)
		}
		total += time.Since(started)
		close(stop)
		<-done
	}
	b.StopTimer()
	b.ReportMetric(float64(keyCount), "keys/op")
	b.ReportMetric(float64(partitions), "partitions/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "snapshot_ns/op")
	b.ReportMetric(float64(maxPause.Load()), "max_read_pause_ns/op")
}

func benchmarkBigWinsAntiEntropy(b *testing.B) {
	keyCount := bigWinsBenchmarkKeys(10000)
	trie := CreateHatTrie()
	targetTrie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.Cleanup(targetTrie.Destroy)
	for idx := 0; idx < keyCount; idx++ {
		key := bigWinsKey(idx)
		trie.UpsertString(key, "value")
		targetTrie.UpsertString(key, "value")
	}
	var targetHandler http.Handler
	var requests atomic.Int64
	var wireBytes atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		r.Body = benchmarkCountingReadCloser{ReadCloser: r.Body, bytes: &wireBytes}
		targetHandler.ServeHTTP(benchmarkCountingResponseWriter{ResponseWriter: w, bytes: &wireBytes}, r)
	}))
	b.Cleanup(target.Close)
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: target.URL},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	targetHandler = NewMonitoringHandler(targetTrie, MonitoringOptions{
		NodeName:          "node-b",
		Topology:          topology,
		ReplicationSafety: NewReplicationSafetyStore(),
	}).Handler()
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Topology: topology, Client: target.Client()})
	b.Cleanup(replicator.Close)
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		result := replicator.SyncAll(context.Background(), trie, "")
		total += time.Since(started)
		if result.Skipped || result.Entries != keyCount {
			b.Fatalf("SyncAll() = %#v, want %d entries", result, keyCount)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(keyCount), "keys/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*keyCount), "ns/key")
	b.ReportMetric(float64(requests.Load())/float64(b.N), "requests/op")
	b.ReportMetric(float64(wireBytes.Load())/float64(b.N), "wire_B/op")
	b.ReportMetric(float64(wireBytes.Load())/float64(b.N*keyCount), "wire_B/key")
}

func benchmarkBigWinsUnaryCommand(b *testing.B) {
	benchmarkBigWinsCommand(b, newGRPCBenchmarkExecutor)
}

func benchmarkBigWinsStreamCommand(b *testing.B) {
	benchmarkBigWinsCommand(b, newGRPCStreamBenchmarkExecutor)
}

func benchmarkBigWinsCommand(b *testing.B, newExecutor func(*testing.B) (benchmarkCommandExecutor, func())) {
	operations := bigWinsBenchmarkOperations(1000)
	execute, stop := newExecutor(b)
	defer stop()
	benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "SETSTR", Key: "command:key", Value: "value"})
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		for operation := 0; operation < operations; operation++ {
			response := execute(CacheCommandRequest{Command: "GET", Key: "command:key"})
			if !response.OK || response.Value != "value" {
				b.Fatalf("unary GET = %#v", response)
			}
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
}

func benchmarkBigWinsPipelinedStreamCommand(b *testing.B) {
	operations := bigWinsBenchmarkOperations(1000)
	wire := &benchmarkGRPCWireStats{}
	client, stop := newGRPCBenchmarkClient(b, grpc.WithStatsHandler(wire))
	defer stop()
	stream, err := client.CommandStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer stream.CloseSend()
	setup, err := cacheCommandRequestToProto(CacheCommandRequest{Command: "SETSTR", Key: "command:key", Value: "value"})
	if err != nil {
		b.Fatal(err)
	}
	if err := stream.Send(setup); err != nil {
		b.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetOk() {
		b.Fatalf("stream setup response = %#v/%v", response, err)
	}
	wire.outbound.Store(0)
	wire.inbound.Store(0)

	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		sendErr := make(chan error, 1)
		go func() {
			for operation := 0; operation < operations; operation++ {
				request, err := cacheCommandRequestToProto(CacheCommandRequest{Command: "GET", Key: "command:key"})
				if err == nil {
					err = stream.Send(request)
				}
				if err != nil {
					sendErr <- err
					return
				}
			}
			sendErr <- nil
		}()
		for operation := 0; operation < operations; operation++ {
			response, err := stream.Recv()
			if err != nil || !response.GetOk() || response.GetValue() != "value" {
				b.Fatalf("pipelined stream GET %d = %#v/%v", operation, response, err)
			}
		}
		if err := <-sendErr; err != nil {
			b.Fatalf("pipelined stream send error = %v", err)
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
	b.ReportMetric(float64(wire.outbound.Load()+wire.inbound.Load())/float64(b.N*operations), "wire_B/command")
}

func benchmarkBigWinsNativeBatchStreamCommand(b *testing.B) {
	const batchSize = 16
	operations := bigWinsBenchmarkOperations(1000)
	wire := &benchmarkGRPCWireStats{}
	client, stop := newGRPCBenchmarkClient(b, grpc.WithStatsHandler(wire))
	defer stop()
	if response, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "command:key", Value: "value"}); err != nil || !response.GetOk() {
		b.Fatalf("batch stream setup = %#v/%v", response, err)
	}
	stream, err := client.CommandBatchStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer stream.CloseSend()
	wire.outbound.Store(0)
	wire.inbound.Store(0)
	batches := (operations + batchSize - 1) / batchSize
	requests := make([]*hatriecachev1.CommandBatchRequest, batches)
	for batch := range requests {
		count := batchSize
		if remaining := operations - batch*batchSize; remaining < count {
			count = remaining
		}
		commands := make([]*hatriecachev1.CommandRequest, count)
		for index := range commands {
			commands[index] = &hatriecachev1.CommandRequest{Command: "GET", Key: "command:key"}
		}
		requests[batch] = &hatriecachev1.CommandBatchRequest{BatchId: uint64(batch + 1), Requests: commands}
	}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		sendErr := make(chan error, 1)
		go func() {
			for _, request := range requests {
				if err := stream.Send(request); err != nil {
					sendErr <- err
					return
				}
			}
			sendErr <- nil
		}()
		received := 0
		for batch := 0; batch < batches; batch++ {
			response, err := stream.Recv()
			if err != nil || !response.GetOk() || response.GetBatchId() != uint64(batch+1) {
				b.Fatalf("native batch stream response %d = %#v/%v", batch, response, err)
			}
			for _, item := range response.GetResponses() {
				if !item.GetOk() || item.GetValue() != "value" {
					b.Fatalf("native batch response item = %#v", item)
				}
			}
			received += len(response.GetResponses())
		}
		if err := <-sendErr; err != nil {
			b.Fatalf("native batch stream send error = %v", err)
		}
		if received != operations {
			b.Fatalf("native batch stream responses = %d, want %d", received, operations)
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(batches), "messages/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
	b.ReportMetric(float64(wire.outbound.Load()+wire.inbound.Load())/float64(b.N*operations), "wire_B/command")
}

func benchmarkBigWinsScalarBatchStreamCommand(b *testing.B) {
	const batchSize = 16
	operations := bigWinsBenchmarkOperations(1000)
	wire := &benchmarkGRPCWireStats{}
	client, stop := newGRPCBenchmarkClient(b, grpc.WithStatsHandler(wire))
	defer stop()
	stream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer stream.CloseSend()
	if err := stream.Send(&hatriecachev1.ScalarBatchRequest{
		BatchId:      1,
		Operations:   []hatriecachev1.ScalarCommand{hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING},
		Keys:         []string{"command:key"},
		StringValues: [][]byte{[]byte("value")},
	}); err != nil {
		b.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetOk() || len(response.GetStatuses()) != 1 || response.GetStatuses()[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
		b.Fatalf("scalar batch setup response = %#v/%v", response, err)
	}
	wire.outbound.Store(0)
	wire.inbound.Store(0)
	batches := (operations + batchSize - 1) / batchSize
	requests := make([]*hatriecachev1.ScalarBatchRequest, batches)
	for batch := range requests {
		count := batchSize
		if remaining := operations - batch*batchSize; remaining < count {
			count = remaining
		}
		operationsColumn := make([]hatriecachev1.ScalarCommand, count)
		keys := make([]string, count)
		for index := range operationsColumn {
			operationsColumn[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
			keys[index] = "command:key"
		}
		requests[batch] = &hatriecachev1.ScalarBatchRequest{BatchId: uint64(batch + 2), Operations: operationsColumn, Keys: keys}
	}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		sendErr := make(chan error, 1)
		go func() {
			for _, request := range requests {
				if err := stream.Send(request); err != nil {
					sendErr <- err
					return
				}
			}
			sendErr <- nil
		}()
		received := 0
		for batch := 0; batch < batches; batch++ {
			response, err := stream.Recv()
			if err != nil || !response.GetOk() || response.GetBatchId() != uint64(batch+2) {
				b.Fatalf("scalar batch stream response %d = %#v/%v", batch, response, err)
			}
			if len(response.GetStatuses()) != len(requests[batch].GetOperations()) || len(response.GetValueEnds()) != len(requests[batch].GetOperations()) {
				b.Fatalf("scalar batch response columns = %#v", response)
			}
			start := uint32(0)
			for index, status := range response.GetStatuses() {
				end := response.GetValueEnds()[index]
				if status != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.GetValueKinds()[index] != hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BYTES || string(response.GetValues()[start:end]) != "value" {
					b.Fatalf("scalar batch response item %d = status %v kind %v value %q", index, status, response.GetValueKinds()[index], response.GetValues()[start:end])
				}
				start = end
			}
			received += len(response.GetStatuses())
		}
		if err := <-sendErr; err != nil {
			b.Fatalf("scalar batch stream send error = %v", err)
		}
		if received != operations {
			b.Fatalf("scalar batch stream responses = %d, want %d", received, operations)
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(batches), "messages/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
	b.ReportMetric(float64(wire.outbound.Load()+wire.inbound.Load())/float64(b.N*operations), "wire_B/command")
}

func benchmarkBigWinsNativeStructuredBatchStreamCommand(b *testing.B) {
	const batchSize = 16
	operations := bigWinsBenchmarkOperations(1000)
	wire := &benchmarkGRPCWireStats{}
	client, stop := newGRPCBenchmarkClient(b, grpc.WithStatsHandler(wire))
	defer stop()
	stream, err := client.CommandBatchStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer stream.CloseSend()
	wire.outbound.Store(0)
	wire.inbound.Store(0)
	batches := (operations + batchSize - 1) / batchSize
	requests := make([]*hatriecachev1.CommandBatchRequest, batches)
	for batch := range requests {
		count := batchSize
		if remaining := operations - batch*batchSize; remaining < count {
			count = remaining
		}
		commands := make([]*hatriecachev1.CommandRequest, count)
		for index := range commands {
			commands[index] = structuredBenchmarkCommand((batch*batchSize + index) % 8)
		}
		requests[batch] = &hatriecachev1.CommandBatchRequest{BatchId: uint64(batch + 1), Requests: commands}
	}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		for batch, request := range requests {
			if err := stream.Send(request); err != nil {
				b.Fatalf("native structured batch send %d = %v", batch, err)
			}
			response, err := stream.Recv()
			if err != nil || !response.GetOk() || len(response.GetResponses()) != len(request.GetRequests()) {
				b.Fatalf("native structured batch response %d = %#v/%v", batch, response, err)
			}
			for index, item := range response.GetResponses() {
				if !item.GetOk() {
					b.Fatalf("native structured batch item %d/%d = %#v", batch, index, item)
				}
			}
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(batches), "messages/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
	b.ReportMetric(float64(wire.outbound.Load()+wire.inbound.Load())/float64(b.N*operations), "wire_B/command")
}

func structuredBenchmarkCommand(index int) *hatriecachev1.CommandRequest {
	priority := int64(7)
	switch index {
	case 0:
		return &hatriecachev1.CommandRequest{Command: "PUTMAP", Key: "structured:map", Subkey: "field", Value: "value"}
	case 1:
		return &hatriecachev1.CommandRequest{Command: "PEEKMAP", Key: "structured:map", Subkey: "field"}
	case 2:
		return &hatriecachev1.CommandRequest{Command: "PUSHSLICE", Key: "structured:slice", Value: "value"}
	case 3:
		return &hatriecachev1.CommandRequest{Command: "POPSLICE", Key: "structured:slice"}
	case 4:
		return &hatriecachev1.CommandRequest{Command: "ADDSET", Key: "structured:set", Value: "value"}
	case 5:
		return &hatriecachev1.CommandRequest{Command: "HASSET", Key: "structured:set", Value: "value"}
	case 6:
		return &hatriecachev1.CommandRequest{Command: "PUSHPQ", Key: "structured:pq", Value: "value", Priority: &priority}
	default:
		return &hatriecachev1.CommandRequest{Command: "POPPQ", Key: "structured:pq"}
	}
}

func benchmarkBigWinsStructuredBatchStreamCommand(b *testing.B) {
	const batchSize = 16
	operations := bigWinsBenchmarkOperations(1000)
	wire := &benchmarkGRPCWireStats{}
	client, stop := newGRPCBenchmarkClient(b, grpc.WithStatsHandler(wire))
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer stream.CloseSend()
	wire.outbound.Store(0)
	wire.inbound.Store(0)
	batches := (operations + batchSize - 1) / batchSize
	requests := make([]*hatriecachev1.StructuredBatchRequest, batches)
	for batch := range requests {
		count := batchSize
		if remaining := operations - batch*batchSize; remaining < count {
			count = remaining
		}
		requests[batch] = structuredBenchmarkRequest(uint64(batch+1), batch*batchSize, count)
	}
	var total time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		for batch, request := range requests {
			if err := stream.Send(request); err != nil {
				b.Fatalf("structured batch send %d = %v", batch, err)
			}
			response, err := stream.Recv()
			if err != nil || !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
				b.Fatalf("structured batch response %d = %#v/%v", batch, response, err)
			}
			for index, status := range response.GetStatuses() {
				if status != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
					b.Fatalf("structured batch item %d/%d = %v", batch, index, status)
				}
			}
		}
		total += time.Since(started)
	}
	b.StopTimer()
	b.ReportMetric(float64(operations), "commands/op")
	b.ReportMetric(float64(batches), "messages/op")
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*operations), "ns/command")
	b.ReportMetric(float64(wire.outbound.Load()+wire.inbound.Load())/float64(b.N*operations), "wire_B/command")
}

func structuredBenchmarkRequest(batchID uint64, start int, count int) *hatriecachev1.StructuredBatchRequest {
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId:    batchID,
		Operations: make([]hatriecachev1.StructuredCommand, count),
		Keys:       make([]string, count),
	}
	for index := 0; index < count; index++ {
		switch (start + index) % 8 {
		case 0:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP
			request.Keys[index] = "structured:map"
			request.Subkeys = append(request.Subkeys, "field")
			request.Values = append(request.Values, []byte("value"))
		case 1:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP
			request.Keys[index] = "structured:map"
			request.Subkeys = append(request.Subkeys, "field")
		case 2:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE
			request.Keys[index] = "structured:slice"
			request.Values = append(request.Values, []byte("value"))
		case 3:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE
			request.Keys[index] = "structured:slice"
		case 4:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET
			request.Keys[index] = "structured:set"
			request.Values = append(request.Values, []byte("value"))
		case 5:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET
			request.Keys[index] = "structured:set"
			request.Values = append(request.Values, []byte("value"))
		case 6:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY
			request.Keys[index] = "structured:pq"
			request.Values = append(request.Values, []byte("value"))
			request.Priorities = append(request.Priorities, 7)
		default:
			request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY
			request.Keys[index] = "structured:pq"
		}
	}
	return request
}

func bigWinsBenchmarkKeys(fallback int) int {
	return bigWinsBenchmarkInt("HATRIE_BIG_WINS_KEYS", fallback)
}

func bigWinsBenchmarkOperations(fallback int) int {
	return bigWinsBenchmarkInt("HATRIE_BIG_WINS_OPS", fallback)
}

func bigWinsBenchmarkSnapshotPartitions() int {
	value, err := strconv.Atoi(os.Getenv("HATRIE_BIG_WINS_SNAPSHOT_PARTITIONS"))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func bigWinsBenchmarkInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func bigWinsKey(idx int) string {
	return fmt.Sprintf("key:%09d", idx)
}

func updateAtomicMax(target *atomic.Int64, value int64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
