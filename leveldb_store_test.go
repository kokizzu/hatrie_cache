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
	if count != 6 {
		t.Fatalf("loaded count = %d, want 6", count)
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
	if got := loaded.TTL("ttl"); got <= 0 || got > time.Minute {
		t.Fatalf("ttl = %s, want remaining positive TTL", got)
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
