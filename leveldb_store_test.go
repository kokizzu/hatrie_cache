package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLevelDBStoreRoundTripRestoresValuesAndTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4000, 0)
	source.now = func() time.Time { return now }

	source.UpsertCounter("counter", -7)
	source.UpsertString("string", "value")
	source.UpsertBytes("bytes", []byte{0, 1, 2, 3})
	source.UpsertMap("map", Map{"name": "ivi", "age": json.Number("32")})
	source.UpsertSlice("slice", Slice{"a", json.Number("2")})
	source.UpsertSet("set", Set{"a", json.Number("2"), "a"})
	source.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 5, Value: json.Number("2")}, {Priority: 1, Value: "urgent"}})
	if err := source.UpsertBloomFilter("bloom", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	source.AddBloomFilter("bloom", "alpha", "beta")
	if err := source.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	source.AddCuckooFilter("cuckoo", "alpha", "beta")
	source.UpsertString("ttl", "alive")
	if !source.Expire("ttl", time.Minute) {
		t.Fatal("Expire(ttl) = false, want true")
	}

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(10 * time.Second) }
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 10 {
		t.Fatalf("loaded count = %d, want 10", count)
	}

	if got := loaded.GetCounter("counter"); got != -7 {
		t.Fatalf("counter = %d, want -7", got)
	}
	if got := loaded.GetString("string"); got != "value" {
		t.Fatalf("string = %q, want value", got)
	}
	if got := loaded.GetBytes("bytes"); !bytes.Equal(got, []byte{0, 1, 2, 3}) {
		t.Fatalf("bytes = %v, want [0 1 2 3]", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"name": "ivi", "age": json.Number("32")}) {
		t.Fatalf("map = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"a", json.Number("2")}) {
		t.Fatalf("slice = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"a", json.Number("2")}) {
		t.Fatalf("set = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetPriorityQueue("priority"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 1, Value: "urgent"}, {Priority: 5, Value: json.Number("2")}}) {
		t.Fatalf("priority queue = %#v, want restored priority order", got)
	}
	if !loaded.HasBloomFilter("bloom", "alpha") || !loaded.HasBloomFilter("bloom", "beta") {
		t.Fatal("loaded Bloom filter does not contain inserted values")
	}
	if !loaded.HasCuckooFilter("cuckoo", "alpha") || !loaded.HasCuckooFilter("cuckoo", "beta") {
		t.Fatal("loaded Cuckoo filter does not contain inserted values")
	}
	if got := loaded.TTL("ttl"); got <= 0 || got > time.Minute {
		t.Fatalf("ttl = %s, want remaining positive TTL", got)
	}
}

func TestLevelDBStoreRoundTripPreservesBlankKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("", "empty")
	source.UpsertString(" ", "space")
	source.UpsertString("\t", "tab")

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("loaded count = %d, want 3", count)
	}

	for key, want := range map[string]string{
		"":   "empty",
		" ":  "space",
		"\t": "tab",
	} {
		if got := loaded.GetString(key); got != want {
			t.Fatalf("GetString(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestLevelDBStoreRoundTripRestoresKeyStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4500, 0)
	source.now = func() time.Time { return now }

	source.UpsertBytes("hot", []byte("value"))
	now = now.Add(time.Second)
	if got := source.GetBytes("hot"); !bytes.Equal(got, []byte("value")) {
		t.Fatalf("GetBytes(hot) = %q, want value", got)
	}
	now = now.Add(time.Second)
	if got := source.GetMap("hot"); got != nil {
		t.Fatalf("GetMap(hot) = %#v, want nil", got)
	}
	want, ok := source.StatsForKey("hot")
	if !ok {
		t.Fatal("StatsForKey(hot) = false, want true")
	}

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(time.Hour) }
	if _, err := loaded.LoadLevelDB(path); err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	got, ok := loaded.StatsForKey("hot")
	if !ok {
		t.Fatal("loaded StatsForKey(hot) = false, want true")
	}
	if got != want {
		t.Fatalf("loaded key stats = %#v, want %#v", got, want)
	}
}

func TestSnapshotOperationValueSizeSupportsPriorityQueue(t *testing.T) {
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: 0, Value: "urgent"},
				{Priority: 5, Sequence: 1, Value: json.Number("2")},
			},
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(priority_queue) error = %v", err)
	}
	if size == 0 {
		t.Fatal("snapshotOperationValueSize(priority_queue) = 0, want encoded size")
	}
}

func TestSnapshotOperationValueSizeSupportsBloomFilter(t *testing.T) {
	filter, err := newBloomFilterData(100, 0.01)
	if err != nil {
		t.Fatalf("newBloomFilterData() error = %v", err)
	}
	snapshot := filter.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:        "bloom_filter",
			BloomFilter: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(bloom_filter) error = %v", err)
	}
	if size != filter.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(bloom_filter) = %d, want %d", size, filter.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsCuckooFilter(t *testing.T) {
	filter, err := newCuckooFilterData(100, 0.01)
	if err != nil {
		t.Fatalf("newCuckooFilterData() error = %v", err)
	}
	snapshot := filter.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:         "cuckoo_filter",
			CuckooFilter: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(cuckoo_filter) error = %v", err)
	}
	if size != filter.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(cuckoo_filter) = %d, want %d", size, filter.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsCountMinSketch(t *testing.T) {
	sketch, err := newCountMinSketchData(128, 4)
	if err != nil {
		t.Fatalf("newCountMinSketchData() error = %v", err)
	}
	snapshot := sketch.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:           "count_min_sketch",
			CountMinSketch: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(count_min_sketch) error = %v", err)
	}
	if size != sketch.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(count_min_sketch) = %d, want %d", size, sketch.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsHyperLogLog(t *testing.T) {
	hll, err := newHyperLogLogData(10)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	snapshot := hll.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:        "hyperloglog",
			HyperLogLog: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(hyperloglog) error = %v", err)
	}
	if size != hll.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(hyperloglog) = %d, want %d", size, hll.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsTopK(t *testing.T) {
	top, err := newTopKData(3)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	top.Add("alpha", 5)
	snapshot := top.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type: "top_k",
			TopK: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(top_k) error = %v", err)
	}
	if size != top.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(top_k) = %d, want %d", size, top.EncodedSize())
	}
}

func TestLevelDBShouldHotLoadRejectsNegativeLimits(t *testing.T) {
	now := time.Unix(4600, 0)
	operation := snapshotOperation{
		entry: snapshotEntry{
			Type:   "string",
			String: "hot",
			Stats: &KeyStats{
				Hits:    1,
				LastHit: now,
			},
		},
	}
	policy := LevelDBLoadPolicy{
		HotValuesOnly: true,
		MaxValueBytes: 1024,
		MaxLastHitAge: time.Hour,
		MinHits:       1,
	}

	if !levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(valid policy) = false, want true")
	}

	policy.MaxValueBytes = -1
	if levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(negative max bytes) = true, want false")
	}

	policy.MaxValueBytes = 1024
	policy.MaxLastHitAge = -time.Second
	if levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(negative max age) = true, want false")
	}
}

func TestLevelDBStoreHotLoadKeepsColdReferencesAndHydratesOnAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4600, 0)
	source.now = func() time.Time { return now }

	source.UpsertString("cold", "cold-value")
	source.UpsertString("hot", "hot-value")
	source.UpsertString("large-hot", string(bytes.Repeat([]byte("x"), 2048)))
	policy := DefaultLevelDBHotLoadPolicy()
	for i := uint64(0); i < policy.MinHits; i++ {
		if got := source.GetString("hot"); got != "hot-value" {
			t.Fatalf("GetString(hot) = %q, want hot-value", got)
		}
		if got := source.GetString("large-hot"); got == "" {
			t.Fatal("GetString(large-hot) = empty, want value")
		}
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(30 * time.Minute) }
	result, err := store.LoadWithPolicy(loaded, policy)
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 3 || result.ValuesLoaded != 1 {
		t.Fatalf("hot-load result = %#v, want 3 keys and 1 value", result)
	}
	valuesByKey := map[string]HatValue{}
	for _, entry := range loaded.Entries(true) {
		valuesByKey[entry.Key] = entry.Value
	}
	if !valuesByKey["hot"].IsStringAtRaws() {
		t.Fatalf("hot value = %+v, want in-memory string", valuesByKey["hot"])
	}
	if !valuesByKey["cold"].IsLevelDBReference() {
		t.Fatalf("cold value = %+v, want leveldb reference", valuesByKey["cold"])
	}
	if !valuesByKey["large-hot"].IsLevelDBReference() {
		t.Fatalf("large-hot value = %+v, want leveldb reference", valuesByKey["large-hot"])
	}
	if !loaded.Exists("cold") {
		t.Fatal("Exists(cold) = false, want true")
	}
	for _, entry := range loaded.Entries(true) {
		if entry.Key == "cold" && !entry.Value.IsLevelDBReference() {
			t.Fatalf("Exists(cold) hydrated value to %+v, want leveldb reference", entry.Value)
		}
	}

	if got := loaded.GetString("cold"); got != "cold-value" {
		t.Fatalf("hydrated cold value = %q, want cold-value", got)
	}
	if hval := loaded.Get("cold"); !hval.IsStringAtRaws() {
		t.Fatalf("cold after hydration = %+v, want in-memory string", hval)
	}
	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(after hot-load) error = %v", err)
	}

	roundTrip := newTestTrie(t)
	if count, err := store.Load(roundTrip); err != nil || count != 3 {
		t.Fatalf("Load(roundTrip) = %d/%v, want 3 nil", count, err)
	}
	if got := roundTrip.GetString("large-hot"); got == "" {
		t.Fatal("large-hot was not preserved after saving hot-loaded trie")
	}
}

func TestLevelDBStoreCloseIsIdempotentAndRejectsOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("key", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := store.Save(loaded); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Save(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := store.Load(loaded); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Load(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("LoadWithPolicy(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := store.Entry("key"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Entry(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
}

func TestHydrateLevelDBReferencesAllowsClosingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "cold-value")
	source.UpsertMap("map", Map{"field": "value"})
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 2 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 2 keys and 0 values", result)
	}
	if got, err := loaded.HydrateLevelDBReferences(); err != nil || got != 2 {
		t.Fatalf("HydrateLevelDBReferences() = %d/%v, want 2/nil", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := loaded.GetString("cold"); got != "cold-value" {
		t.Fatalf("GetString(cold) after close = %q, want cold-value", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"field": "value"}) {
		t.Fatalf("GetMap(map) after close = %#v, want hydrated map", got)
	}
	path = filepath.Join(t.TempDir(), "snapshot.json")
	if err := loaded.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot(after close) error = %v", err)
	}
}

func TestHydrateLevelDBReferencesReportsClosedStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got, err := loaded.HydrateLevelDBReferences(); got != 0 || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HydrateLevelDBReferences() = %d/%v, want 0/ErrLevelDBStoreClosed", got, err)
	}
	if err := loaded.SaveSnapshot(filepath.Join(t.TempDir(), "snapshot.json")); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("SaveSnapshot(with closed cold ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
}

func TestLevelDBStoreHotLoadCanDeleteColdReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "value")
	source.UpsertString("keep", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 2 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 2 keys and 0 values", result)
	}
	if !loaded.Delete("cold") {
		t.Fatal("Delete(cold reference) = false, want true")
	}
	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(after delete) error = %v", err)
	}

	roundTrip := newTestTrie(t)
	if count, err := store.Load(roundTrip); err != nil || count != 1 {
		t.Fatalf("Load(roundTrip) = %d/%v, want 1 nil", count, err)
	}
	if got := roundTrip.GetString("cold"); got != "" {
		t.Fatalf("deleted cold value = %q, want empty", got)
	}
	if got := roundTrip.GetString("keep"); got != "value" {
		t.Fatalf("keep = %q, want value", got)
	}
}

func TestLevelDBStoreHotLoadSchedulesColdReferenceExpiration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4700, 0)
	source.now = func() time.Time { return now }
	source.UpsertString("cold", "value")
	if !source.Expire("cold", time.Minute) {
		t.Fatal("Expire(cold) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 1 key and 0 values", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || entries[0].Key != "cold" || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want cold leveldb reference", entries)
	}

	now = now.Add(2 * time.Minute)
	if got := loaded.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() after hot-load = %d, want 1", got)
	}
	if got := loaded.GetString("cold"); got != "" {
		t.Fatalf("cold after vacuum = %q, want empty", got)
	}
}

func TestLevelDBStoreSkipsExpiredValuesOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(5000, 0)
	source.now = func() time.Time { return now }

	source.UpsertString("expired", "old")
	if !source.Expire("expired", time.Second) {
		t.Fatal("Expire(expired) = false, want true")
	}
	source.UpsertString("active", "new")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(2 * time.Second) }
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("loaded count = %d, want 1", count)
	}
	if got := loaded.GetString("expired"); got != "" {
		t.Fatalf("expired = %q, want empty", got)
	}
	if got := loaded.GetString("active"); got != "new" {
		t.Fatalf("active = %q, want new", got)
	}
}

func TestLevelDBStoreRestoresLargeBytesToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	payload := testPayload(DiskBytesThreshold + 1)
	source.UpsertBytes("large", payload)

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	if _, err := loaded.LoadLevelDB(path); err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	hval := loaded.Get("large")
	if !hval.IsBytesAtRaws() || !hval.OnDisk() {
		t.Fatalf("loaded large value = %+v, want on-disk bytes", hval)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("large payload mismatch after LevelDB load")
	}
}

func TestLevelDBStoreSaveRemovesStaleKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("keep", "value")
	source.UpsertString("stale", "old")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(first) error = %v", err)
	}

	if !source.Delete("stale") {
		t.Fatal("Delete(stale) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(second) error = %v", err)
	}

	loaded := newTestTrie(t)
	if count, err := loaded.LoadLevelDB(path); err != nil || count != 1 {
		t.Fatalf("LoadLevelDB() = %d/%v, want 1 nil", count, err)
	}
	if got := loaded.GetString("keep"); got != "value" {
		t.Fatalf("keep = %q, want value", got)
	}
	if got := loaded.GetString("stale"); got != "" {
		t.Fatalf("stale = %q, want empty", got)
	}
}

func TestDecodeLevelDBEntryRejectsInvalidJSON(t *testing.T) {
	if _, err := decodeLevelDBEntry([]byte(`{"key":"x","type":"string"} trailing`)); err == nil {
		t.Fatal("decodeLevelDBEntry(trailing) error = nil, want error")
	}
	if _, err := decodeLevelDBEntry([]byte(`{"type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntry(missing key) error = nil, want error")
	}
	if _, err := decodeLevelDBEntry([]byte(`{"key":null,"type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntry(null key) error = nil, want error")
	}
	if _, err := decodeLevelDBEntryForKey("x", []byte(`{"key":"y","type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntryForKey(mismatch) error = nil, want error")
	}
	if entry, err := decodeLevelDBEntryForKey("", []byte(`{"key":"","type":"string","string":"value"}`)); err != nil || entry.Key != "" {
		t.Fatalf("decodeLevelDBEntryForKey(empty key) = %#v/%v, want empty key/nil", entry, err)
	}
}
