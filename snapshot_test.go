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
	if got := loaded.TTL("string"); got != time.Minute {
		t.Fatalf("TTL(string) = %s, want 1m", got)
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

func TestLoadSnapshotRejectsInvalidInput(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()

	for name, payload := range map[string]string{
		"broken":   `{broken`,
		"version":  `{"version":999,"entries":[]}`,
		"type":     `{"version":1,"entries":[{"key":"bad","type":"unknown"}]}`,
		"trailing": `{"version":1,"entries":[]} trailing`,
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
