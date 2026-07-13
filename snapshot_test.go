package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
	if err := ht.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	ht.AddCuckooFilter("cuckoo", "alpha", "beta")
	ht.UpsertRoaringBitmap("bitmap")
	ht.AddRoaringBitmap("bitmap", 1, 1<<16+7)
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
	if !loaded.HasCuckooFilter("cuckoo", "alpha") || !loaded.HasCuckooFilter("cuckoo", "beta") {
		t.Fatal("loaded Cuckoo filter does not contain inserted values")
	}
	if info, ok := loaded.CuckooFilterInfo("cuckoo"); !ok || info.Count != 2 || info.Capacity < 128 {
		t.Fatalf("loaded CuckooFilterInfo = %#v/%v, want restored Cuckoo filter", info, ok)
	}
	if got := loaded.GetRoaringBitmap("bitmap"); !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7}) {
		t.Fatalf("loaded Roaring bitmap = %#v, want restored integer set", got)
	}
	if info, ok := loaded.RoaringBitmapInfo("bitmap"); !ok || info.Cardinality != 2 || info.Containers != 2 {
		t.Fatalf("loaded RoaringBitmapInfo = %#v/%v, want restored Roaring bitmap", info, ok)
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

func TestLoadSnapshotRejectsInvalidInput(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()

	for name, payload := range map[string]string{
		"broken":      `{broken`,
		"version":     `{"version":999,"entries":[]}`,
		"type":        `{"version":1,"entries":[{"key":"bad","type":"unknown"}]}`,
		"missing-key": `{"version":1,"entries":[{"type":"string","string":"value"}]}`,
		"null-key":    `{"version":1,"entries":[{"key":null,"type":"string","string":"value"}]}`,
		"trailing":    `{"version":1,"entries":[]} trailing`,
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
