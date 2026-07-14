package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRoundTripRestoresValuesAndTTL(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertCounter("counter", 42)
	ht.UpsertString("string", "value")
	ht.UpsertBytes("bytes", []byte("payload"))
	ht.UpsertMap("map", Map{"name": "ivi", "age": json.Number("32")})
	ht.UpsertSlice("slice", Slice{"a", json.Number("2")})
	ht.UpsertSet("set", Set{"a", json.Number("2"), "a"})
	ht.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 5, Value: json.Number("2")}, {Priority: 1, Value: "urgent"}})
	if err := ht.UpsertBloomFilter("bloom", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	ht.AddBloomFilter("bloom", "alpha", "beta")
	if err := ht.UpsertCountMinSketch("freq", 128, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	ht.IncrementCountMinSketch("freq", "alpha", 5)
	if err := ht.UpsertHyperLogLog("card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	ht.AddHyperLogLog("card", "alpha", "beta")
	if err := ht.UpsertTopK("top", 3); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	ht.AddTopK("top", "alpha", 5)
	if err := ht.UpsertQuantileSketch("latency", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	ht.AddQuantileSketch("latency", 10, 20, 30)
	if err := ht.UpsertFenwickTree("scores", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	ht.AddFenwickTree("scores", 2, 5)
	ht.AddFenwickTree("scores", 6, 7)
	if err := ht.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	ht.AddCuckooFilter("cuckoo", "alpha", "beta")
	if err := ht.UpsertXorFilter("xor", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	if _, err := ht.AddXorFilter("xor", "alpha", "beta"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := ht.BuildXorFilter("xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %v/%v, want ok", err, ok)
	}
	ht.UpsertRadixTree("radix")
	ht.PutRadixTree("radix", "user:100/profile", Map{"status": "active"})
	ht.PutRadixTree("radix", "user:101/profile", json.Number("42"))
	ht.UpsertRoaringBitmap("bitmap")
	ht.AddRoaringBitmap("bitmap", 1, 1<<16+7)
	ht.UpsertSparseBitset("bitset")
	ht.AddSparseBitset("bitset", 1, 1<<32+7, ^uint64(0))
	if err := ht.UpsertReservoirSample("sample", 3); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	ht.AddReservoirSample("sample", "alpha", "beta", "gamma", "delta")
	if !ht.Expire("string", time.Minute) {
		t.Fatal("Expire(string) = false, want true")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if got := loaded.GetCounter("counter"); got != 42 {
		t.Fatalf("counter = %d, want 42", got)
	}
	if got := loaded.GetString("string"); got != "value" {
		t.Fatalf("string = %q, want value", got)
	}
	if got := loaded.GetBytes("bytes"); !bytes.Equal(got, []byte("payload")) {
		t.Fatalf("bytes = %q, want payload", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"name": "ivi", "age": json.Number("32")}) {
		t.Fatalf("map = %#v, want restored map", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"a", json.Number("2")}) {
		t.Fatalf("slice = %#v, want restored slice", got)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"a", json.Number("2")}) {
		t.Fatalf("set = %#v, want restored set", got)
	}
	if got := loaded.GetPriorityQueue("priority"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 1, Value: "urgent"}, {Priority: 5, Value: json.Number("2")}}) {
		t.Fatalf("priority queue = %#v, want restored priority order", got)
	}
	if !loaded.HasBloomFilter("bloom", "alpha") || !loaded.HasBloomFilter("bloom", "beta") {
		t.Fatal("loaded Bloom filter does not contain inserted values")
	}
	if info, ok := loaded.BloomFilterInfo("bloom"); !ok || info.Insertions != 2 {
		t.Fatalf("loaded BloomFilterInfo = %#v/%v, want 2 insertions", info, ok)
	}
	if got, ok := loaded.EstimateCountMinSketch("freq", "alpha"); !ok || got != 5 {
		t.Fatalf("loaded Count-Min Sketch estimate = %d/%v, want 5", got, ok)
	}
	if info, ok := loaded.CountMinSketchInfo("freq"); !ok || info.Width != 128 || info.Depth != 4 || info.TotalCount != 5 {
		t.Fatalf("loaded CountMinSketchInfo = %#v/%v, want restored sketch", info, ok)
	}
	if got, ok := loaded.CountHyperLogLog("card"); !ok || got < 2 {
		t.Fatalf("loaded HyperLogLog estimate = %d/%v, want at least 2", got, ok)
	}
	if info, ok := loaded.HyperLogLogInfo("card"); !ok || info.Precision != 10 || info.Observations != 2 {
		t.Fatalf("loaded HyperLogLogInfo = %#v/%v, want restored HyperLogLog", info, ok)
	}
	if got := loaded.EstimateTopK("top", "alpha"); !got.Tracked || got.Count != 5 {
		t.Fatalf("loaded Top-K estimate = %#v, want alpha count 5", got)
	}
	if info, ok := loaded.TopKInfo("top"); !ok || info.Capacity != 3 || info.Total != 5 {
		t.Fatalf("loaded TopKInfo = %#v/%v, want restored Top-K", info, ok)
	}
	if got, ok := loaded.EstimateQuantileSketch("latency", 0.5); !ok || got.Count != 3 || got.Value < 10 || got.Value > 30 {
		t.Fatalf("loaded quantile estimate = %#v/%v, want restored sketch", got, ok)
	}
	if info, ok := loaded.QuantileSketchInfo("latency"); !ok || info.Epsilon != 0.01 || info.Count != 3 {
		t.Fatalf("loaded QuantileSketchInfo = %#v/%v, want restored quantile sketch", info, ok)
	}
	if got, ok := loaded.PrefixSumFenwickTree("scores", 6); !ok || got != 12 {
		t.Fatalf("loaded Fenwick prefix sum = %d/%v, want 12", got, ok)
	}
	if got, ok := loaded.RangeSumFenwickTree("scores", 3, 6); !ok || got != 7 {
		t.Fatalf("loaded Fenwick range sum = %d/%v, want 7", got, ok)
	}
	if info, ok := loaded.FenwickTreeInfo("scores"); !ok || info.Size != 8 || info.Updates != 2 || info.Total != 12 {
		t.Fatalf("loaded FenwickTreeInfo = %#v/%v, want restored Fenwick tree", info, ok)
	}
	if !loaded.HasCuckooFilter("cuckoo", "alpha") || !loaded.HasCuckooFilter("cuckoo", "beta") {
		t.Fatal("loaded Cuckoo filter does not contain inserted values")
	}
	if info, ok := loaded.CuckooFilterInfo("cuckoo"); !ok || info.Count != 2 || info.Capacity < 128 {
		t.Fatalf("loaded CuckooFilterInfo = %#v/%v, want restored Cuckoo filter", info, ok)
	}
	if hit, queryable := loaded.HasXorFilter("xor", "alpha"); !queryable || !hit {
		t.Fatalf("loaded XOR filter alpha = %v/%v, want hit", hit, queryable)
	}
	if info, ok := loaded.XorFilterInfo("xor"); !ok || !info.Built || info.Items != 2 {
		t.Fatalf("loaded XorFilterInfo = %#v/%v, want restored XOR filter", info, ok)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:100/profile"); !ok || !reflect.DeepEqual(value, Map{"status": "active"}) {
		t.Fatalf("loaded radix user:100/profile = %#v/%v, want restored nested value", value, ok)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:101/profile"); !ok || value != json.Number("42") {
		t.Fatalf("loaded radix user:101/profile = %#v/%v, want restored json.Number", value, ok)
	}
	if info, ok := loaded.RadixTreeInfo("radix"); !ok || info.Items != 2 || info.Nodes == 0 {
		t.Fatalf("loaded RadixTreeInfo = %#v/%v, want restored radix tree", info, ok)
	}
	if got := loaded.GetRoaringBitmap("bitmap"); !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7}) {
		t.Fatalf("loaded Roaring bitmap = %#v, want restored integer set", got)
	}
	if info, ok := loaded.RoaringBitmapInfo("bitmap"); !ok || info.Cardinality != 2 || info.Containers != 2 {
		t.Fatalf("loaded RoaringBitmapInfo = %#v/%v, want restored Roaring bitmap", info, ok)
	}
	if got := loaded.GetSparseBitset("bitset"); !reflect.DeepEqual(got, []uint64{1, 1<<32 + 7, ^uint64(0)}) {
		t.Fatalf("loaded sparse bitset = %#v, want restored uint64 set", got)
	}
	if info, ok := loaded.SparseBitsetInfo("bitset"); !ok || info.Cardinality != 3 || info.Containers != 3 {
		t.Fatalf("loaded SparseBitsetInfo = %#v/%v, want restored sparse bitset", info, ok)
	}
	if got := loaded.GetReservoirSample("sample"); len(got) != 3 {
		t.Fatalf("loaded reservoir sample len = %d, want bounded sample capacity 3: %#v", len(got), got)
	}
	if info, ok := loaded.ReservoirSampleInfo("sample"); !ok || info.Capacity != 3 || info.Tracked != 3 || info.Seen != 4 {
		t.Fatalf("loaded ReservoirSampleInfo = %#v/%v, want restored reservoir sample", info, ok)
	}
	if got := loaded.TTL("string"); got != time.Minute {
		t.Fatalf("TTL(string) = %s, want 1m", got)
	}
}

func TestSnapshotRoundTripPreservesBlankKeys(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "empty")
	ht.UpsertString(" ", "space")
	ht.UpsertString("\t", "tab")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
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

func TestPriorityQueueSnapshotPreservesTieOrderAndNextSequence(t *testing.T) {
	ht := newTestTrie(t)
	if added := ht.PushPriorityQueue("jobs", 1, "first", "second"); added != 2 {
		t.Fatalf("PushPriorityQueue(first, second) = %d, want 2", added)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if added := loaded.PushPriorityQueue("jobs", 1, "third"); added != 1 {
		t.Fatalf("PushPriorityQueue(third) = %d, want 1", added)
	}

	for _, want := range []string{"first", "second", "third"} {
		got, ok := loaded.PopPriorityQueue("jobs")
		if !ok || got.Priority != 1 || got.Value != want {
			t.Fatalf("PopPriorityQueue() = %#v/%v, want %q priority 1", got, ok, want)
		}
	}
}

func TestWriteFileAtomicReplacesFileAndCleansTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")

	if err := writeFileAtomic(path, []byte("first")); err != nil {
		t.Fatalf("writeFileAtomic(first) error = %v", err)
	}
	if err := writeFileAtomic(path, []byte("second")); err != nil {
		t.Fatalf("writeFileAtomic(second) error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("file payload = %q, want second", data)
	}
	assertNoAtomicTempFiles(t, dir, "data.json")
}

func TestWriteFileAtomicCleansTemporaryFileOnRenameError(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target.json")
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	if err := writeFileAtomic(targetDir, []byte("payload")); err == nil {
		t.Fatal("writeFileAtomic(directory target) error = nil, want error")
	}
	assertNoAtomicTempFiles(t, dir, "target.json")
	if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
		t.Fatalf("target directory = %v/%v, want existing directory", info, err)
	}
}

func TestSnapshotRoundTripRestoresKeyStats(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2050, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("hot", "value")
	now = now.Add(time.Second)
	if got := ht.GetString("hot"); got != "value" {
		t.Fatalf("GetString(hot) = %q, want value", got)
	}
	now = now.Add(time.Second)
	if got := ht.GetCounter("hot"); got != 0 {
		t.Fatalf("GetCounter(hot) = %d, want 0", got)
	}
	want, ok := ht.StatsForKey("hot")
	if !ok {
		t.Fatal("StatsForKey(hot) = false, want true")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(time.Hour) }
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	got, ok := loaded.StatsForKey("hot")
	if !ok {
		t.Fatal("loaded StatsForKey(hot) = false, want true")
	}
	if got != want {
		t.Fatalf("loaded key stats = %#v, want %#v", got, want)
	}
}

func TestLoadSnapshotWithoutKeyStatsDoesNotInventStats(t *testing.T) {
	ht := newTestTrie(t)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "legacy", Type: "string", String: "value"},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if stats, ok := ht.StatsForKey("legacy"); ok {
		t.Fatalf("StatsForKey(legacy) = %#v, true; want false", stats)
	}
	if got := ht.GetString("legacy"); got != "value" {
		t.Fatalf("legacy value = %q, want value", got)
	}
}

func TestSnapshotRoundTripRestoresLargeBytesToDisk(t *testing.T) {
	ht := newTestTrie(t)
	payload := testPayload(DiskBytesThreshold + 1)
	ht.UpsertBytes("large", payload)

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	hval := loaded.Get("large")
	if !hval.OnDisk() {
		t.Fatalf("large snapshot value metadata = %+v, want on disk", hval)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("large bytes changed after snapshot restore")
	}
}

func TestLoadSnapshotSkipsExpiredEntries(t *testing.T) {
	now := time.Unix(3000, 0)
	expiredAt := now.Add(-time.Second)
	activeUntil := now.Add(time.Minute)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "expired", Type: "string", String: "old", ExpiresAt: &expiredAt},
			{Key: "active", Type: "bytes", Bytes: base64.StdEncoding.EncodeToString([]byte("live")), ExpiresAt: &activeUntil},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := ht.GetString("expired"); got != "" {
		t.Fatalf("expired snapshot entry = %q, want skipped", got)
	}
	if got := ht.GetBytes("active"); !bytes.Equal(got, []byte("live")) {
		t.Fatalf("active snapshot entry = %q, want live", got)
	}
}

func TestLoadSnapshotUsesSingleExpirationClock(t *testing.T) {
	base := time.Unix(3025, 0)
	expiresAt := base.Add(time.Second)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "soon", Type: "string", String: "value", ExpiresAt: &expiresAt},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	clockReads := 0
	ht.now = func() time.Time {
		clockReads++
		if clockReads == 1 {
			return base
		}
		return base.Add(2 * time.Second)
	}
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if clockReads != 1 {
		t.Fatalf("LoadSnapshot clock reads = %d, want one captured load time", clockReads)
	}
	ht.now = func() time.Time { return base }
	if got := ht.GetString("soon"); got != "value" {
		t.Fatalf("soon after snapshot load = %q, want value", got)
	}
}

func TestLoadSnapshotRemovesKeysMissingFromSnapshot(t *testing.T) {
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "keep", Type: "string", String: "value"},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("stale", "old")
	ht.UpsertBytes("stale-large", testPayload(DiskBytesThreshold+1))
	staleValue := ht.Get("stale-large")
	stalePath := ht.disks.paths[staleValue.Index]
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := ht.GetString("keep"); got != "value" {
		t.Fatalf("keep after snapshot load = %q, want value", got)
	}
	if got := ht.GetString("stale"); got != "" {
		t.Fatalf("stale after snapshot load = %q, want empty", got)
	}
	if got := ht.GetBytes("stale-large"); got != nil {
		t.Fatalf("stale-large after snapshot load = %q, want nil", got)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale disk file Stat() error = %v, want not exist", err)
	}
}

func TestLoadSnapshotCleansCreatedKeysAfterApplyFailure(t *testing.T) {
	payload := testPayload(DiskBytesThreshold + 1)
	encoded := base64.StdEncoding.EncodeToString(payload)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "a-created", Type: "bytes", Bytes: encoded},
			{Key: "b-blocked", Type: "bytes", Bytes: encoded},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	firstPath := ht.disks.pathFor(0)
	blockedPath := ht.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want blocked disk write error")
	}
	if ht.Exists("a-created") {
		t.Fatal("failed snapshot load left created key a-created")
	}
	if ht.Exists("b-blocked") {
		t.Fatal("failed snapshot load left blocked key b-blocked")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after failed snapshot load = %q, want keep", got)
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("created disk file Stat() error = %v, want not exist", err)
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed snapshot load = %d, want 0", got)
	}
}

func TestLoadSnapshotRollsBackExistingKeysAfterApplyFailure(t *testing.T) {
	payload := testPayload(DiskBytesThreshold + 1)
	encoded := base64.StdEncoding.EncodeToString(payload)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "a-existing", Type: "bytes", Bytes: encoded},
			{Key: "b-blocked", Type: "bytes", Bytes: encoded},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("a-existing", "keep")
	firstPath := ht.disks.pathFor(0)
	blockedPath := ht.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want blocked disk write error")
	}
	if got := ht.GetString("a-existing"); got != "keep" {
		t.Fatalf("a-existing after failed snapshot load = %q, want keep", got)
	}
	if ht.Exists("b-blocked") {
		t.Fatal("failed snapshot load left blocked key b-blocked")
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("rolled-back disk file Stat() error = %v, want not exist", err)
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed snapshot rollback = %d, want 0", got)
	}
}

func TestLoadSnapshotSchedulesExpirationForVacuum(t *testing.T) {
	now := time.Unix(3050, 0)
	expiresAt := now.Add(time.Minute)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "ttl", Type: "string", String: "value", ExpiresAt: &expiresAt},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	now = now.Add(2 * time.Minute)
	if got := ht.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() after snapshot load = %d, want 1", got)
	}
	if got := ht.GetString("ttl"); got != "" {
		t.Fatalf("ttl after vacuum = %q, want empty", got)
	}
}

func TestLoadSnapshotAcceptsEmptyCollectionPayloads(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"map","type":"map","map":{}},{"key":"slice","type":"slice","slice":[]},{"key":"set","type":"set","set":[]},{"key":"priority","type":"priority_queue","priority_queue":[]}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if got, ok, err := ht.GetMapChecked("map"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetMapChecked(map) = %#v/%v/%v, want empty map", got, ok, err)
	}
	if got, ok, err := ht.GetSliceChecked("slice"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetSliceChecked(slice) = %#v/%v/%v, want empty slice", got, ok, err)
	}
	if got, ok, err := ht.GetSetChecked("set"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetSetChecked(set) = %#v/%v/%v, want empty set", got, ok, err)
	}
	if got, ok, err := ht.GetPriorityQueueChecked("priority"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetPriorityQueueChecked(priority) = %#v/%v/%v, want empty priority queue", got, ok, err)
	}
}

func TestLoadSnapshotRejectsInvalidInput(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()

	for name, payload := range map[string]string{
		"broken":           `{broken`,
		"version":          `{"version":999,"entries":[]}`,
		"type":             `{"version":1,"entries":[{"key":"bad","type":"unknown"}]}`,
		"missing-key":      `{"version":1,"entries":[{"type":"string","string":"value"}]}`,
		"null-key":         `{"version":1,"entries":[{"key":null,"type":"string","string":"value"}]}`,
		"map-missing":      `{"version":1,"entries":[{"key":"bad","type":"map"}]}`,
		"map-null":         `{"version":1,"entries":[{"key":"bad","type":"map","map":null}]}`,
		"slice-missing":    `{"version":1,"entries":[{"key":"bad","type":"slice"}]}`,
		"set-missing":      `{"version":1,"entries":[{"key":"bad","type":"set"}]}`,
		"priority-missing": `{"version":1,"entries":[{"key":"bad","type":"priority_queue"}]}`,
		"priority-null":    `{"version":1,"entries":[{"key":"bad","type":"priority_queue","priority_queue":null}]}`,
		"trailing":         `{"version":1,"entries":[]} trailing`,
	} {
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
		if err := ht.LoadSnapshot(path); err == nil {
			t.Fatalf("LoadSnapshot(%s) error = nil, want error", name)
		}
	}
}

func TestLoadSnapshotRejectsInvalidCollectionsWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"bad","type":"map"}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want invalid collection payload error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected snapshot = %q, want keep", got)
	}
	if ht.Exists("bad") {
		t.Fatal("invalid snapshot created bad key")
	}
}

func TestLoadSnapshotRejectsDuplicateActiveKeysWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"dup","type":"string","string":"first"},{"key":"dup","type":"string","string":"second"}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "duplicate active key") {
		t.Fatalf("LoadSnapshot(duplicate key) error = %v, want duplicate active key error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected duplicate snapshot = %q, want keep", got)
	}
	if ht.Exists("dup") {
		t.Fatal("duplicate snapshot created dup key")
	}
}

func TestLoadSnapshotRejectsTooLongKey(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	data, err := json.Marshal(snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: strings.Repeat("k", maxHATTrieKeyLength+1), Type: "string", String: "bad"},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("LoadSnapshot(too-long key) error = %v, want key length error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing value after rejected snapshot = %q, want keep", got)
	}
	if got := ht.Size(); got != 1 {
		t.Fatalf("size after rejected snapshot = %d, want 1", got)
	}
}

func TestValidateSnapshotEntryRejectsUnsupportedCollectionValues(t *testing.T) {
	unsupported := func() {}
	tests := []snapshotEntry{
		{Key: "map", Type: "map", Map: Map{"bad": unsupported}},
		{Key: "slice", Type: "slice", Slice: Slice{unsupported}},
		{Key: "set", Type: "set", Set: Set{unsupported}},
		{Key: "priority", Type: "priority_queue", PriorityQueue: []priorityQueueItem{{Value: unsupported}}},
	}
	for _, entry := range tests {
		if _, err := validateSnapshotEntry(entry); err == nil {
			t.Fatalf("validateSnapshotEntry(%s) error = nil, want unsupported value error", entry.Type)
		}
	}
}

func TestValidateSnapshotEntryRejectsInconsistentKeyStats(t *testing.T) {
	entry := snapshotEntry{
		Key:    "bad-stats",
		Type:   "string",
		String: "value",
		Stats: &KeyStats{
			Reads:  1,
			Hits:   2,
			Misses: 0,
		},
	}
	if _, err := validateSnapshotEntry(entry); err == nil || !strings.Contains(err.Error(), "key stats reads must equal hits plus misses") {
		t.Fatalf("validateSnapshotEntry(inconsistent key stats) error = %v, want key stats read count error", err)
	}
}

func TestLoadSnapshotRejectsInconsistentKeyStatsWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"bad-stats","type":"string","string":"value","stats":{"reads":1,"hits":2,"misses":0}}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "key stats reads must equal hits plus misses") {
		t.Fatalf("LoadSnapshot(inconsistent key stats) error = %v, want key stats read count error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected key stats snapshot = %q, want keep", got)
	}
	if ht.Exists("bad-stats") {
		t.Fatal("invalid key stats snapshot created bad-stats key")
	}
}

func TestValidateSnapshotEntryRejectsCorruptPriorityQueueSequences(t *testing.T) {
	tests := []snapshotEntry{
		{
			Key:  "duplicate",
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: 7, Value: "first"},
				{Priority: 1, Sequence: 7, Value: "second"},
			},
		},
		{
			Key:  "overflow",
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: ^uint64(0), Value: "last"},
			},
		},
	}
	for _, entry := range tests {
		if _, err := validateSnapshotEntry(entry); err == nil {
			t.Fatalf("validateSnapshotEntry(%s) error = nil, want corrupt priority queue sequence error", entry.Key)
		}
	}
}

func TestLoadSnapshotRejectsCorruptPriorityQueueWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"jobs","type":"priority_queue","priority_queue":[{"priority":1,"sequence":2,"value":"first"},{"priority":1,"sequence":2,"value":"second"}]}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want corrupt priority queue error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected snapshot = %q, want keep", got)
	}
	if ht.Exists("jobs") {
		t.Fatal("invalid priority queue snapshot created jobs key")
	}
}

func TestSaveSnapshotRejectsUnsupportedJSONValues(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertMap("bad", Map{"ch": make(chan int)})

	if err := ht.SaveSnapshot(filepath.Join(t.TempDir(), "snapshot.json")); err == nil {
		t.Fatal("SaveSnapshot() error = nil, want unsupported JSON value error")
	}
}

func assertNoAtomicTempFiles(t *testing.T, dir string, base string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, base+".tmp-*"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files remain: %v", matches)
	}
}
