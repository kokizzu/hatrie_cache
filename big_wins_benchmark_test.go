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
)

const bigWinsDurableWrites = 100

func BenchmarkBigWins(b *testing.B) {
	b.Run("GlobalTelemetry", benchmarkBigWinsGlobalTelemetry)
	b.Run("ConcurrentRead", benchmarkBigWinsConcurrentRead)
	b.Run("ConcurrentWrite", benchmarkBigWinsConcurrentWrite)
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
	for idx := 0; idx < keyCount; idx++ {
		trie.UpsertString(bigWinsKey(idx), "snapshot-value")
	}
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
				_ = trie.GetString(bigWinsKey(0))
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
	client, stop := newGRPCBenchmarkClient(b)
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
}

func bigWinsBenchmarkKeys(fallback int) int {
	return bigWinsBenchmarkInt("HATRIE_BIG_WINS_KEYS", fallback)
}

func bigWinsBenchmarkOperations(fallback int) int {
	return bigWinsBenchmarkInt("HATRIE_BIG_WINS_OPS", fallback)
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
