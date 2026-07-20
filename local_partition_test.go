package hatriecache

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestLocalPartitionsDefaultOffAndValidateConfiguration(t *testing.T) {
	trie := newTestTrie(t)
	if stats := trie.LocalPartitioningStats(); stats.Enabled || stats.Partitions != 0 || len(stats.Sizes) != 0 {
		t.Fatalf("default local partition stats = %#v, want disabled", stats)
	}
	for _, count := range []int{-1, 1, 3, MaxLocalPartitions + 1} {
		if err := trie.ConfigureLocalPartitions(count); err == nil {
			t.Fatalf("ConfigureLocalPartitions(%d) error = nil, want validation error", count)
		}
	}
	if err := trie.ConfigureLocalPartitions(0); err != nil {
		t.Fatalf("ConfigureLocalPartitions(0) error = %v", err)
	}
	trie.UpsertString("existing", "value")
	if err := trie.ConfigureLocalPartitions(8); err == nil {
		t.Fatal("ConfigureLocalPartitions() on nonempty trie error = nil")
	}
}

func TestLocalPartitionsRouteBasicOperationsAndMergeScans(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	trie.now = func() time.Time { return now }
	wantKeys := make([]string, 0, 130)
	seenPartitions := map[int]struct{}{}
	for index := 0; index < 128; index++ {
		key := "partition:key:" + strconv.Itoa(index)
		partition, enabled, err := trie.LocalPartitionForKey(key)
		if err != nil || !enabled {
			t.Fatalf("LocalPartitionForKey(%q) = %d/%v/%v, want enabled", key, partition, enabled, err)
		}
		seenPartitions[partition] = struct{}{}
		trie.UpsertString(key, "value-"+strconv.Itoa(index))
		wantKeys = append(wantKeys, key)
	}
	if len(seenPartitions) < 2 {
		t.Fatalf("128 keys used %d local partition, want distribution", len(seenPartitions))
	}
	trie.UpsertCounter("partition:counter", 40)
	trie.IncrementCounter("partition:counter", 2)
	wantKeys = append(wantKeys, "partition:counter")
	trie.UpsertString("partition:ttl", "temporary")
	if !trie.Expire("partition:ttl", time.Minute) {
		t.Fatal("Expire(partition:ttl) = false")
	}
	wantKeys = append(wantKeys, "partition:ttl")

	if got := trie.GetString("partition:key:73"); got != "value-73" {
		t.Fatalf("GetString(partition:key:73) = %q", got)
	}
	if got := trie.GetCounter("partition:counter"); got != 42 {
		t.Fatalf("GetCounter(partition:counter) = %d, want 42", got)
	}
	if ttl := trie.TTL("partition:ttl"); ttl != time.Minute {
		t.Fatalf("TTL(partition:ttl) = %s, want 1m", ttl)
	}
	if !trie.Delete("partition:key:9") || trie.Exists("partition:key:9") {
		t.Fatal("partitioned delete did not remove key")
	}
	wantKeys = removeTestString(wantKeys, "partition:key:9")
	sort.Strings(wantKeys)
	if got := trie.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("partitioned Keys(true) mismatch: got %d keys, want %d", len(got), len(wantKeys))
	}
	if got := trie.Size(); got != len(wantKeys) {
		t.Fatalf("partitioned Size() = %d, want %d", got, len(wantKeys))
	}
	stats := trie.LocalPartitioningStats()
	if !stats.Enabled || stats.Partitions != 8 || sumTestInts(stats.Sizes) != len(wantKeys) {
		t.Fatalf("local partition stats = %#v, want eight partitions and %d keys", stats, len(wantKeys))
	}
}

func TestLocalPartitionsExecuteCommandsAndCrossPartitionBatchInOrder(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	requests := make([]CacheCommandRequest, 0, 192)
	for index := 0; index < 64; index++ {
		key := "batch:key:" + strconv.Itoa(index)
		requests = append(requests, CacheCommandRequest{Command: "SETSTR", Key: key, Value: strconv.Itoa(index)})
	}
	for index := 0; index < 64; index++ {
		key := "batch:key:" + strconv.Itoa(index)
		requests = append(requests, CacheCommandRequest{Command: "GET", Key: key})
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Batch: requests})
	if !response.OK || len(response.Responses) != len(requests) {
		t.Fatalf("partitioned BATCH envelope = %#v", response)
	}
	for index := 0; index < 64; index++ {
		if got := response.Responses[64+index].Value; got != strconv.Itoa(index) {
			t.Fatalf("partitioned BATCH GET %d = %q", index, got)
		}
	}
	put := trie.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}})
	peek := trie.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "city"})
	if !put.OK || !peek.OK || peek.Value != "Singapore" {
		t.Fatalf("partitioned structured commands = %#v/%#v", put, peek)
	}
}

func TestLocalPartitionsSnapshotRoundTripRemainsPortable(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("name", "ivi")
	source.UpsertCounter("count", 42)
	if response := source.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}}); !response.OK {
		t.Fatalf("PUTMAP response = %#v", response)
	}
	path := filepath.Join(t.TempDir(), "partitioned.snapshot")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}

	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	if err := partitioned.LoadSnapshot(path); err != nil {
		t.Fatalf("partitioned LoadSnapshot() error = %v", err)
	}
	assertLocalPartitionSnapshotValues(t, partitioned)

	plain := newTestTrie(t)
	if err := plain.LoadSnapshot(path); err != nil {
		t.Fatalf("plain LoadSnapshot() error = %v", err)
	}
	assertLocalPartitionSnapshotValues(t, plain)
}

func TestLocalPartitionsPebbleRoundTrip(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("name", "ivi")
	source.UpsertCounter("count", 42)
	path := filepath.Join(t.TempDir(), "cache.pebble")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(source); err != nil {
		store.Close()
		t.Fatalf("Pebble Save() error = %v", err)
	}

	restored := newTestTrie(t)
	if err := restored.ConfigureLocalPartitions(8); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if _, err := store.LoadWithPolicy(restored, LevelDBLoadPolicy{HotValuesOnly: true}); err != nil {
		store.Close()
		t.Fatalf("Pebble LoadWithPolicy() error = %v", err)
	}
	assertLocalPartitionScalarValues(t, restored)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalPartitionsMonitoringAndScalarGRPC(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	entries := trie.monitoringEntriesPage("session:", "", false, 10)
	if len(entries.Entries) != 2 {
		t.Fatalf("partitioned monitoring entries = %#v, want two", entries)
	}
	if got := []string{entries.Entries[0].Key, entries.Entries[1].Key}; !reflect.DeepEqual(got, []string{"session:1", "session:2"}) {
		t.Fatalf("partitioned monitoring keys = %v", got)
	}
	metrics := NewMonitoringHandler(trie, MonitoringOptions{}).prometheusMetrics()
	if !strings.Contains(metrics, "hatrie_cache_local_partitions{node=\"") || !strings.Contains(metrics, "hatrie_cache_local_partition_keys{") {
		t.Fatalf("partition metrics are missing:\n%s", metrics)
	}

	client, stop := newTestGRPCClient(t, trie, CacheGRPCOptions{})
	defer stop()
	stream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.ScalarBatchRequest{
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
		},
		Keys:          []string{"grpc:count", "grpc:count", "grpc:count"},
		IntegerValues: []int64{40, 2},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || string(response.GetValues()) != "42" || !reflect.DeepEqual(response.GetIntegerValues(), []int64{42}) {
		t.Fatalf("partitioned scalar gRPC response = %#v/%v", response, err)
	}
}

func TestLocalPartitionsRouteAllDirectValueFamilies(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}

	trie.UpsertBytes("direct:bytes", []byte("payload"))
	if got := string(trie.GetBytes("direct:bytes")); got != "payload" {
		t.Fatalf("bytes = %q", got)
	}
	if got := trie.Get("direct:bytes"); !got.IsBytesAtRaws() {
		t.Fatalf("generic Get() = %#v, want bytes", got)
	}
	trie.UpsertMap("direct:map", Map{"field": "value"})
	if got := trie.PeekMap("direct:map", "field"); got != "value" {
		t.Fatalf("map field = %#v", got)
	}
	trie.PushSlice("direct:slice", "first", "last")
	if got := trie.TailSlice("direct:slice"); got != "last" {
		t.Fatalf("slice tail = %#v", got)
	}
	trie.AddSet("direct:set", "member")
	if !trie.HasSet("direct:set", "member") {
		t.Fatal("set member is missing")
	}
	trie.PushPriorityQueue("direct:pq", 1, "job")
	if item, ok := trie.PeekPriorityQueue("direct:pq"); !ok || item.Value != "job" {
		t.Fatalf("priority queue peek = %#v/%v", item, ok)
	}
	trie.AddBloomFilter("direct:bloom", "member")
	if !trie.HasBloomFilter("direct:bloom", "member") {
		t.Fatal("bloom member is missing")
	}
	trie.AddCuckooFilter("direct:cuckoo", "member")
	if !trie.HasCuckooFilter("direct:cuckoo", "member") {
		t.Fatal("cuckoo member is missing")
	}
	if _, err := trie.AddXorFilter("direct:xor", "member"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := trie.BuildXorFilter("direct:xor"); err != nil || !ok {
		t.Fatalf("xor build = %v/%v", ok, err)
	}
	if hit, queryable := trie.HasXorFilter("direct:xor", "member"); !hit || !queryable {
		t.Fatalf("xor member = %v/%v", hit, queryable)
	}
	trie.AddRoaringBitmap("direct:roaring", 42)
	if !trie.HasRoaringBitmap("direct:roaring", 42) {
		t.Fatal("roaring member is missing")
	}
	trie.AddSparseBitset("direct:sparse", 1<<40)
	if !trie.HasSparseBitset("direct:sparse", 1<<40) {
		t.Fatal("sparse member is missing")
	}
	trie.PutRadixTree("direct:radix", "session:42", "active")
	if got, ok := trie.GetRadixTree("direct:radix", "session:42"); !ok || got != "active" {
		t.Fatalf("radix value = %#v/%v", got, ok)
	}
	trie.IncrementCountMinSketch("direct:cms", "path", 3)
	if got, ok := trie.EstimateCountMinSketch("direct:cms", "path"); !ok || got < 3 {
		t.Fatalf("count-min estimate = %d/%v", got, ok)
	}
	trie.AddHyperLogLog("direct:hll", "visitor")
	if got, ok := trie.CountHyperLogLog("direct:hll"); !ok || got == 0 {
		t.Fatalf("hyperloglog count = %d/%v", got, ok)
	}
	trie.AddTopK("direct:topk", "path", 3)
	if got := trie.EstimateTopK("direct:topk", "path"); !got.Tracked || got.Count < 3 {
		t.Fatalf("top-k estimate = %#v", got)
	}
	trie.AddReservoirSample("direct:reservoir", "request")
	if got := trie.GetReservoirSample("direct:reservoir"); len(got) != 1 {
		t.Fatalf("reservoir items = %#v", got)
	}
	trie.AddQuantileSketch("direct:quantile", 42)
	if got, ok := trie.EstimateQuantileSketch("direct:quantile", 0.5); !ok || got.Value != 42 {
		t.Fatalf("quantile estimate = %#v/%v", got, ok)
	}
	if _, ok := trie.AddFenwickTree("direct:fenwick", 5, 42); !ok {
		t.Fatal("fenwick add failed")
	}
	if got, ok := trie.RangeSumFenwickTree("direct:fenwick", 0, 5); !ok || got != 42 {
		t.Fatalf("fenwick range sum = %d/%v", got, ok)
	}

	if got := trie.Size(); got != 17 {
		t.Fatalf("partitioned direct value family size = %d, want 17", got)
	}
}

func TestLocalPartitionsMaintenancePaths(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		trie.UpsertString("maintenance:"+strconv.Itoa(index), "value")
	}

	compacted, err := trie.CompactMemory()
	if err != nil || compacted.Entries != 64 {
		t.Fatalf("CompactMemory() = %#v/%v", compacted, err)
	}
	merkle, err := trie.replicationMerkleSnapshot()
	if err != nil || merkle.count != 64 {
		t.Fatalf("replicationMerkleSnapshot() = %d/%v", merkle.count, err)
	}
	var replicated []string
	page, err := replicationSyncEntriesPage(trie, "maintenance:", "", false, 10, func(entry Entry) error {
		replicated = append(replicated, entry.Key)
		return nil
	})
	if err != nil || len(replicated) != 10 || !page.hasMore {
		t.Fatalf("replication page = %#v/%v, keys=%d", page, err, len(replicated))
	}
}

func TestLocalPartitionsColdSpillAcrossStorageBackends(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			trie := newTestTrie(t)
			if err := trie.ConfigureLocalPartitions(8); err != nil {
				t.Fatal(err)
			}
			value := strings.Repeat("x", 256)
			for index := 0; index < 64; index++ {
				trie.UpsertString("spill:"+strconv.Itoa(index), value)
			}
			store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), backend, StorageFormatBinary)
			if err != nil {
				t.Fatal(err)
			}
			underCap, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 1 << 20, MinValueBytes: 1})
			if err != nil || underCap.KeysSpilled != 0 || underCap.HotBytesAfter != underCap.HotBytesBefore {
				store.Close()
				t.Fatalf("SpillCold(under cap) = %#v/%v", underCap, err)
			}
			result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1})
			if err != nil {
				store.Close()
				t.Fatalf("SpillCold() error = %v", err)
			}
			if result.KeysSpilled != 64 || result.HotBytesAfter != 0 || result.WriteBatches == 0 || result.WriteBatches > 8 {
				store.Close()
				t.Fatalf("SpillCold() = %#v", result)
			}
			if got := trie.GetString("spill:42"); got != value {
				store.Close()
				t.Fatalf("hydrated value length = %d", len(got))
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLocalPartitionsSaveAndLoadAggregateStats(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("stats:a", "a")
	source.UpsertString("stats:b", "b")
	_ = source.GetString("stats:a")
	_ = source.GetString("stats:missing")
	path := filepath.Join(t.TempDir(), "stats.json")
	if err := source.SaveStats(path); err != nil {
		t.Fatal(err)
	}

	target := newTestTrie(t)
	if err := target.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	if err := target.LoadStats(path); err != nil {
		t.Fatal(err)
	}
	got := target.Stats()
	if got.Writes != 2 || got.Reads != 2 || got.Hits != 1 || got.Misses != 1 {
		t.Fatalf("restored aggregate stats = %#v", got)
	}
}

func TestLocalPartitionsBoundGlobalKeyStatsCapacity(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	if err := trie.ConfigureCounterWriteStripes(64); err != nil {
		t.Fatal(err)
	}
	if stats := trie.CounterWriteStripingStats(); !stats.Enabled || stats.Stripes != 64 {
		t.Fatalf("partitioned counter stripes = %#v", stats)
	}
	if err := trie.ConfigureKeyStats(KeyStatsModeBounded, 10); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1_000; index++ {
		key := "key-stats:" + strconv.Itoa(index)
		trie.UpsertString(key, "value")
		_ = trie.GetString(key)
	}
	policy := trie.KeyStatsPolicy()
	if policy.Mode != KeyStatsModeBounded || policy.Capacity != 10 || policy.Tracked > 10 {
		t.Fatalf("partitioned key stats policy = %#v", policy)
	}
}

func assertLocalPartitionSnapshotValues(t *testing.T, trie *HatTrie) {
	t.Helper()
	if trie.GetString("name") != "ivi" || trie.GetCounter("count") != 42 {
		t.Fatalf("restored scalar values = %q/%d", trie.GetString("name"), trie.GetCounter("count"))
	}
	peek := trie.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "city"})
	if !peek.OK || peek.Value != "Singapore" {
		t.Fatalf("restored map response = %#v", peek)
	}
}

func assertLocalPartitionScalarValues(t *testing.T, trie *HatTrie) {
	t.Helper()
	if trie.GetString("name") != "ivi" || trie.GetCounter("count") != 42 {
		t.Fatalf("restored scalar values = %q/%d", trie.GetString("name"), trie.GetCounter("count"))
	}
}

func removeTestString(values []string, remove string) []string {
	for index, value := range values {
		if value == remove {
			return append(values[:index], values[index+1:]...)
		}
	}
	return values
}

func sumTestInts(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}
