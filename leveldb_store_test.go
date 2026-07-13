package hatriecache

import (
	"bytes"
	"encoding/json"
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
	if count != 7 {
		t.Fatalf("loaded count = %d, want 7", count)
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
	if got := loaded.TTL("ttl"); got <= 0 || got > time.Minute {
		t.Fatalf("ttl = %s, want remaining positive TTL", got)
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

func TestLevelDBStoreHotLoadKeepsColdReferencesAndHydratesOnAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4600, 0)
	source.now = func() time.Time { return now }

	source.UpsertString("cold", "cold-value")
	source.UpsertString("hot", "hot-value")
	source.UpsertString("large-hot", string(bytes.Repeat([]byte("x"), 2048)))
	for i := 0; i < 1001; i++ {
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
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
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
}
