package hatriecache

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestTrie(t *testing.T) *HatTrie {
	t.Helper()
	ht := CreateHatTrie()
	t.Cleanup(ht.Destroy)
	return ht
}

func TestHatValueRoundTripPreservesNegativeCountersAndFlags(t *testing.T) {
	in := HatValue{Index: -42, Flags: DATAVALUE_TYPE_COUNTER | (1 << DATAVALUE_TTL_BIT_SHIFT)}
	var out HatValue
	out.FromUlong(in.ToUlong())

	if out != in {
		t.Fatalf("round trip mismatch: got %+v, want %+v", out, in)
	}
	if !out.IsCounter() || !out.HasTtl() {
		t.Fatalf("decoded flags are wrong: %+v", out)
	}
}

func TestCounterOperations(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertCounter("counter", 5)
	ht.IncrementCounter("counter", -7)
	if got := ht.GetCounter("counter"); got != -2 {
		t.Fatalf("GetCounter() = %d, want -2", got)
	}
	if got := ht.GetString("counter"); got != "-2" {
		t.Fatalf("GetString(counter) = %q, want %q", got, "-2")
	}
}

func TestKeysUseFullLengthAndSupportNULBytes(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertString("abcde-one", "one")
	ht.UpsertString("abcde-two", "two")
	ht.UpsertString("nul\x00key", "zero")

	if got := ht.GetString("abcde-one"); got != "one" {
		t.Fatalf("first long key = %q, want one", got)
	}
	if got := ht.GetString("abcde-two"); got != "two" {
		t.Fatalf("second long key = %q, want two", got)
	}
	if got := ht.GetString("nul\x00key"); got != "zero" {
		t.Fatalf("NUL-containing key = %q, want zero", got)
	}
}

func TestStringOperationsReuseStorage(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertString("key", "middle")
	idx := ht.Get("key").Index
	ht.AppendString("key", "-tail")
	ht.PrependString("key", "head-")

	if got := ht.GetString("key"); got != "head-middle-tail" {
		t.Fatalf("GetString() = %q, want head-middle-tail", got)
	}
	if got := ht.Get("key").Index; got != idx {
		t.Fatalf("string update moved storage index: got %d, want %d", got, idx)
	}
	if got := len(ht.raws.array); got != 1 {
		t.Fatalf("raw storage grew during same-type string updates: len=%d", got)
	}
}

func TestBytesOperationsCopyInputsAndOutputs(t *testing.T) {
	ht := newTestTrie(t)

	input := []byte("abc")
	ht.UpsertBytes("bytes", input)
	input[0] = 'x'

	got := ht.GetBytes("bytes")
	if !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("stored bytes changed with caller input: got %q", got)
	}

	got[1] = 'y'
	if again := ht.GetBytes("bytes"); !bytes.Equal(again, []byte("abc")) {
		t.Fatalf("stored bytes changed through returned slice: got %q", again)
	}
}

func TestLargeBytesStoredOnDiskAndCleaned(t *testing.T) {
	ht := newTestTrie(t)

	input := testPayload(DiskBytesThreshold + 1)
	want := cloneBytes(input)
	ht.UpsertBytes("large", input)
	input[0] ^= 0xff

	hval := ht.Get("large")
	if !hval.IsBytesAtRaws() || !hval.OnDisk() {
		t.Fatalf("large bytes metadata = %+v, want on-disk bytes", hval)
	}
	path := ht.disks.paths[hval.Index]
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("large bytes file was not created: %v", err)
	}
	if got := ht.GetBytes("large"); !bytes.Equal(got, want) {
		t.Fatalf("GetBytes(large) changed with caller input")
	}

	got := ht.GetBytes("large")
	got[0] ^= 0xff
	if again := ht.GetBytes("large"); !bytes.Equal(again, want) {
		t.Fatalf("GetBytes(large) exposed mutable disk value")
	}

	if !ht.Delete("large") {
		t.Fatal("Delete(large) = false, want true")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("large bytes file still exists after delete: %v", err)
	}
	if !ht.disks.reusables[hval.Index] {
		t.Fatalf("deleted disk index %d was not reusable", hval.Index)
	}
}

func TestBytesDiskThresholdAndReplacement(t *testing.T) {
	ht := newTestTrie(t)

	thresholdValue := testPayload(DiskBytesThreshold)
	ht.UpsertBytes("bytes", thresholdValue)
	rawIdx := ht.Get("bytes").Index
	if hval := ht.Get("bytes"); hval.OnDisk() {
		t.Fatalf("threshold-sized bytes stored on disk: %+v", hval)
	}

	largeValue := testPayload(DiskBytesThreshold + 1)
	ht.UpsertBytes("bytes", largeValue)
	diskValue := ht.Get("bytes")
	if !diskValue.OnDisk() {
		t.Fatalf("large bytes were not stored on disk: %+v", diskValue)
	}
	if !ht.raws.reusables[rawIdx] {
		t.Fatalf("replaced raw index %d was not reusable", rawIdx)
	}
	diskPath := ht.disks.paths[diskValue.Index]

	smallValue := []byte("small")
	ht.UpsertBytes("bytes", smallValue)
	if hval := ht.Get("bytes"); hval.OnDisk() {
		t.Fatalf("small replacement stayed on disk: %+v", hval)
	}
	if _, err := os.Stat(diskPath); !os.IsNotExist(err) {
		t.Fatalf("disk file still exists after small replacement: %v", err)
	}
	if got := ht.GetBytes("bytes"); !bytes.Equal(got, smallValue) {
		t.Fatalf("small replacement bytes = %q, want %q", got, smallValue)
	}
}

func TestOnDiskBytesExpireAndDestroyCleanFiles(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(50, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertBytes("large", testPayload(DiskBytesThreshold+1))
	hval := ht.Get("large")
	path := ht.disks.paths[hval.Index]
	if !ht.Expire("large", time.Second) {
		t.Fatal("Expire(large) = false, want true")
	}
	now = now.Add(2 * time.Second)
	if got := ht.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() = %d, want 1", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("disk file still exists after expiration vacuum: %v", err)
	}

	owned := CreateHatTrie()
	ownedDir := owned.disks.dir
	owned.UpsertBytes("large", testPayload(DiskBytesThreshold+1))
	owned.Destroy()
	if _, err := os.Stat(ownedDir); !os.IsNotExist(err) {
		t.Fatalf("owned disk directory still exists after destroy: %v", err)
	}
}

func TestMapOperations(t *testing.T) {
	ht := newTestTrie(t)

	ht.PutMap("map", "name", "ivi")
	ht.PutMap("map", "age", 32)

	if hval := ht.Get("map"); !hval.IsMap() {
		t.Fatalf("PutMap on missing key stored type %+v, want map", hval)
	}
	if got := ht.PeekMap("map", "name"); got != "ivi" {
		t.Fatalf("PeekMap() = %v, want ivi", got)
	}
	if got := ht.TakeMap("map", "name"); got != "ivi" {
		t.Fatalf("TakeMap() = %v, want ivi", got)
	}
	if got := ht.PeekMap("map", "name"); got != nil {
		t.Fatalf("PeekMap after TakeMap() = %v, want nil", got)
	}

	copied := ht.GetMap("map")
	copied["age"] = 99
	if got := ht.PeekMap("map", "age"); got != 32 {
		t.Fatalf("GetMap exposed internal map: got age %v, want 32", got)
	}
}

func TestMapJSONSerializerRoundTrip(t *testing.T) {
	input := Map{
		"name": "ivi",
		"age":  json.Number("32"),
		"nested": map[string]interface{}{
			"enabled": true,
			"ratio":   json.Number("1.25"),
		},
		"items": []interface{}{json.Number("1"), "two", false},
	}

	data, err := MarshalMapJSON(input)
	if err != nil {
		t.Fatalf("MarshalMapJSON() error = %v", err)
	}
	output, err := UnmarshalMapJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalMapJSON() error = %v", err)
	}
	if !reflect.DeepEqual(output, input) {
		t.Fatalf("round trip = %#v, want %#v", output, input)
	}
}

func TestMapJSONCacheMethods(t *testing.T) {
	ht := newTestTrie(t)

	input := []byte(`{"age":32,"name":"ivi","nested":{"enabled":true},"items":[1,"two"]}`)
	if err := ht.UpsertMapJSON("json", input); err != nil {
		t.Fatalf("UpsertMapJSON() error = %v", err)
	}

	got := ht.GetMap("json")
	if got["age"] != json.Number("32") {
		t.Fatalf("decoded age = %#v, want json.Number(32)", got["age"])
	}
	nested, ok := got["nested"].(map[string]interface{})
	if !ok || nested["enabled"] != true {
		t.Fatalf("decoded nested map = %#v", got["nested"])
	}

	data, ok, err := ht.GetMapJSON("json")
	if err != nil {
		t.Fatalf("GetMapJSON() error = %v", err)
	}
	if !ok {
		t.Fatal("GetMapJSON() ok = false, want true")
	}
	roundTrip, err := UnmarshalMapJSON(data)
	if err != nil {
		t.Fatalf("GetMapJSON payload did not decode: %v", err)
	}
	if !reflect.DeepEqual(roundTrip, got) {
		t.Fatalf("GetMapJSON round trip = %#v, want %#v", roundTrip, got)
	}

	if data, ok, err := ht.GetMapJSON("missing"); err != nil || ok || data != nil {
		t.Fatalf("GetMapJSON(missing) = (%q, %v, %v), want nil false nil", data, ok, err)
	}
}

func TestMapJSONRejectsInvalidInputs(t *testing.T) {
	for _, input := range [][]byte{
		[]byte(`[]`),
		[]byte(`"not an object"`),
		[]byte(`{"ok":true} trailing`),
		[]byte(`{"broken"`),
	} {
		if got, err := UnmarshalMapJSON(input); err == nil {
			t.Fatalf("UnmarshalMapJSON(%q) = %#v, nil error", input, got)
		}
	}

	if _, err := MarshalMapJSON(Map{"bad": make(chan int)}); err == nil {
		t.Fatal("MarshalMapJSON(unsupported value) error = nil, want error")
	}
}

func TestStatsTrackReadsWritesDeletesAndRates(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(900, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("key", "value")
	now = now.Add(time.Second)
	if got := ht.GetString("key"); got != "value" {
		t.Fatalf("GetString(key) = %q, want value", got)
	}
	now = now.Add(time.Second)
	if got := ht.GetString("missing"); got != "" {
		t.Fatalf("GetString(missing) = %q, want empty", got)
	}
	now = now.Add(time.Second)
	if !ht.Delete("key") {
		t.Fatal("Delete(key) = false, want true")
	}

	stats := ht.Stats()
	if stats.Reads != 2 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("read stats = reads %d hits %d misses %d, want 2/1/1", stats.Reads, stats.Hits, stats.Misses)
	}
	if stats.Writes != 2 {
		t.Fatalf("writes = %d, want 2", stats.Writes)
	}
	if stats.Deletes != 1 {
		t.Fatalf("deletes = %d, want 1", stats.Deletes)
	}
	if stats.HitRate != 0.5 || stats.CumulativeHitRate != 0.5 {
		t.Fatalf("hit rates = %f/%f, want 0.5/0.5", stats.HitRate, stats.CumulativeHitRate)
	}
	if !stats.LastHit.Equal(time.Unix(901, 0)) {
		t.Fatalf("LastHit = %s, want %s", stats.LastHit, time.Unix(901, 0))
	}
	if !stats.LastMiss.Equal(time.Unix(902, 0)) {
		t.Fatalf("LastMiss = %s, want %s", stats.LastMiss, time.Unix(902, 0))
	}
	if !stats.LastWrite.Equal(time.Unix(903, 0)) {
		t.Fatalf("LastWrite = %s, want %s", stats.LastWrite, time.Unix(903, 0))
	}
}

func TestStatsTrackExpirationsAndPersistToDisk(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("key", "value")
	if !ht.Expire("key", time.Second) {
		t.Fatal("Expire(key) = false, want true")
	}
	now = now.Add(2 * time.Second)
	if got := ht.GetString("key"); got != "" {
		t.Fatalf("expired GetString(key) = %q, want empty", got)
	}

	stats := ht.Stats()
	if stats.Expirations != 1 {
		t.Fatalf("expirations = %d, want 1", stats.Expirations)
	}
	if stats.Misses != 1 {
		t.Fatalf("misses = %d, want 1", stats.Misses)
	}

	path := filepath.Join(t.TempDir(), "stats.json")
	if err := ht.SaveStats(path); err != nil {
		t.Fatalf("SaveStats() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadStats(path); err != nil {
		t.Fatalf("LoadStats() error = %v", err)
	}
	if got := loaded.Stats(); !cacheStatsEqual(got, stats) {
		t.Fatalf("loaded stats = %#v, want %#v", got, stats)
	}
}

func TestLoadStatsRejectsInvalidJSON(t *testing.T) {
	ht := newTestTrie(t)
	path := filepath.Join(t.TempDir(), "stats.json")
	if err := os.WriteFile(path, []byte(`{broken`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := ht.LoadStats(path); err == nil {
		t.Fatal("LoadStats(invalid JSON) error = nil, want error")
	}
}

func TestConcurrentStatsUpdatesAreSynchronized(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "value")

	const (
		workers    = 4
		iterations = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = ht.GetString("key")
				_ = ht.GetString("missing")
				ht.UpsertCounter("counter", int32(i))
			}
		}()
	}
	wg.Wait()

	stats := ht.Stats()
	if stats.Reads != workers*iterations*2 {
		t.Fatalf("reads = %d, want %d", stats.Reads, workers*iterations*2)
	}
	if stats.Hits != workers*iterations || stats.Misses != workers*iterations {
		t.Fatalf("hits/misses = %d/%d, want %d/%d", stats.Hits, stats.Misses, workers*iterations, workers*iterations)
	}
}

func TestConcurrentMapJSONOperationsAreSynchronized(t *testing.T) {
	ht := newTestTrie(t)

	const (
		workers    = 4
		iterations = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(`{"worker":` + strconv.Itoa(worker) + `,"iteration":` + strconv.Itoa(i) + `}`)
				if err := ht.UpsertMapJSON("json", data); err != nil {
					t.Errorf("UpsertMapJSON() error = %v", err)
					return
				}
				if _, ok, err := ht.GetMapJSON("json"); err != nil || !ok {
					t.Errorf("GetMapJSON() = ok %v err %v", ok, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestSliceOperations(t *testing.T) {
	ht := newTestTrie(t)

	ht.PushSlice("slice", 1, 2, "three")
	if hval := ht.Get("slice"); !hval.IsSlice() {
		t.Fatalf("PushSlice on missing key stored type %+v, want slice", hval)
	}
	if got := ht.HeadSlice("slice"); got != 1 {
		t.Fatalf("HeadSlice() = %v, want 1", got)
	}
	if got := ht.TailSlice("slice"); got != "three" {
		t.Fatalf("TailSlice() = %v, want three", got)
	}
	if got := ht.ShiftSlice("slice"); got != 1 {
		t.Fatalf("ShiftSlice() = %v, want 1", got)
	}
	if got := ht.PopSlice("slice"); got != "three" {
		t.Fatalf("PopSlice() = %v, want three", got)
	}
	if got := ht.GetSlice("slice"); !reflect.DeepEqual(got, Slice{2}) {
		t.Fatalf("GetSlice() = %#v, want %#v", got, Slice{2})
	}

	copied := ht.GetSlice("slice")
	copied[0] = 99
	if got := ht.HeadSlice("slice"); got != 2 {
		t.Fatalf("GetSlice exposed internal slice: got head %v, want 2", got)
	}
}

func TestPopAndShiftMissingSliceDoNotCreateKeys(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.PopSlice("missing"); got != nil {
		t.Fatalf("PopSlice(missing) = %v, want nil", got)
	}
	if got := ht.ShiftSlice("missing"); got != nil {
		t.Fatalf("ShiftSlice(missing) = %v, want nil", got)
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("missing slice operations created keys: size=%d", got)
	}
}

func TestDeleteReleasesBackingStorageForReuse(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertString("old", "value")
	idx := ht.Get("old").Index
	if !ht.Delete("old") {
		t.Fatal("Delete(old) = false, want true")
	}
	if got := ht.Get("old"); !got.Empty() {
		t.Fatalf("deleted key still exists: %+v", got)
	}
	if !ht.raws.reusables[idx] {
		t.Fatalf("deleted raw index %d was not marked reusable", idx)
	}

	ht.UpsertString("new", "value")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("raw storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestTypeReplacementReleasesPreviousStorage(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertMap("key", Map{"old": true})
	mapIdx := ht.Get("key").Index
	ht.UpsertSlice("key", Slice{"new"})

	if hval := ht.Get("key"); !hval.IsSlice() {
		t.Fatalf("replacement type = %+v, want slice", hval)
	}
	if !ht.maps.reusables[mapIdx] {
		t.Fatalf("replaced map index %d was not marked reusable", mapIdx)
	}
}

func TestTTLExpiresOnReadAndReusesStorage(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(100, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("ttl", "value")
	idx := ht.Get("ttl").Index
	if !ht.Expire("ttl", 10*time.Second) {
		t.Fatal("Expire(ttl) = false, want true")
	}
	if hval := ht.Get("ttl"); !hval.HasTtl() || !hval.IsStringAtRaws() {
		t.Fatalf("TTL flag not set on value: %+v", hval)
	}
	if got := ht.TTL("ttl"); got != 10*time.Second {
		t.Fatalf("TTL() = %s, want 10s", got)
	}

	now = now.Add(11 * time.Second)
	if got := ht.GetString("ttl"); got != "" {
		t.Fatalf("expired GetString() = %q, want empty string", got)
	}
	if got := ht.Get("ttl"); !got.Empty() {
		t.Fatalf("expired key still exists: %+v", got)
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("size after read-time expiration = %d, want 0", got)
	}
	if !ht.raws.reusables[idx] {
		t.Fatalf("expired raw index %d was not reusable", idx)
	}

	ht.UpsertString("new", "value")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("expired raw storage was not reused: got %d, want %d", got, idx)
	}
}

func TestPersistRemovesTTL(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(200, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertCounter("counter", 7)
	if !ht.ExpireAt("counter", now.Add(5*time.Second)) {
		t.Fatal("ExpireAt(counter) = false, want true")
	}
	if !ht.Persist("counter") {
		t.Fatal("Persist(counter) = false, want true")
	}
	if got := ht.TTL("counter"); got != NoTTL {
		t.Fatalf("TTL after Persist() = %s, want NoTTL", got)
	}
	if hval := ht.Get("counter"); hval.HasTtl() {
		t.Fatalf("TTL flag remains after Persist(): %+v", hval)
	}

	now = now.Add(10 * time.Second)
	if got := ht.GetCounter("counter"); got != 7 {
		t.Fatalf("persisted counter expired: got %d, want 7", got)
	}
}

func TestPlainUpsertClearsTTL(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(300, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("key", "old")
	if !ht.Expire("key", time.Minute) {
		t.Fatal("Expire(key) = false, want true")
	}
	ht.UpsertString("key", "new")

	if got := ht.TTL("key"); got != NoTTL {
		t.Fatalf("TTL after plain UpsertString() = %s, want NoTTL", got)
	}
	if hval := ht.Get("key"); hval.HasTtl() {
		t.Fatalf("TTL flag remains after plain upsert: %+v", hval)
	}
}

func TestExpiredReadsAcrossValueFamilies(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(400, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertCounter("counter", 3)
	ht.UpsertBytes("bytes", []byte("value"))
	ht.UpsertMap("map", Map{"field": "value"})
	ht.UpsertSlice("slice", Slice{"value"})
	for _, key := range []string{"counter", "bytes", "map", "slice"} {
		if !ht.Expire(key, time.Second) {
			t.Fatalf("Expire(%q) = false, want true", key)
		}
	}

	now = now.Add(2 * time.Second)
	if got := ht.GetCounter("counter"); got != 0 {
		t.Fatalf("expired counter = %d, want 0", got)
	}
	if got := ht.GetBytes("bytes"); got != nil {
		t.Fatalf("expired bytes = %q, want nil", got)
	}
	if got := ht.PeekMap("map", "field"); got != nil {
		t.Fatalf("expired map field = %v, want nil", got)
	}
	if got := ht.HeadSlice("slice"); got != nil {
		t.Fatalf("expired slice head = %v, want nil", got)
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("size after family expiration reads = %d, want 0", got)
	}
}

func TestTTLAPIsHandleMissingAndImmediateExpiry(t *testing.T) {
	ht := newTestTrie(t)

	if ht.Expire("missing", time.Second) {
		t.Fatal("Expire(missing) = true, want false")
	}
	if ht.Persist("missing") {
		t.Fatal("Persist(missing) = true, want false")
	}
	if got := ht.TTL("missing"); got != NoTTL {
		t.Fatalf("TTL(missing) = %s, want NoTTL", got)
	}

	ht.UpsertString("key", "value")
	if !ht.Expire("key", 0) {
		t.Fatal("Expire(key, 0) = false, want true")
	}
	if got := ht.Get("key"); !got.Empty() {
		t.Fatalf("immediately expired key still exists: %+v", got)
	}
}

func TestKeysAndEntries(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertCounter("banana", 3)
	ht.UpsertString("apple", "red")
	ht.UpsertBytes("apricot", []byte("orange"))
	ht.UpsertMap("map", Map{"field": "value"})
	ht.UpsertSlice("slice", Slice{"item"})

	wantKeys := []string{"apple", "apricot", "banana", "map", "slice"}
	if got := ht.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Keys(sorted) = %#v, want %#v", got, wantKeys)
	}

	entries := ht.Entries(true)
	if got := entryKeys(entries); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Entries(sorted) keys = %#v, want %#v", got, wantKeys)
	}

	values := entriesByKey(entries)
	if got := values["banana"]; !got.IsCounter() || got.Index != 3 {
		t.Fatalf("banana entry = %+v, want counter 3", got)
	}
	if got := values["apple"]; !got.IsStringAtRaws() {
		t.Fatalf("apple entry = %+v, want string", got)
	}
	if got := values["apricot"]; !got.IsBytesAtRaws() {
		t.Fatalf("apricot entry = %+v, want bytes", got)
	}
	if got := values["map"]; !got.IsMap() {
		t.Fatalf("map entry = %+v, want map", got)
	}
	if got := values["slice"]; !got.IsSlice() {
		t.Fatalf("slice entry = %+v, want slice", got)
	}
}

func TestKeysWithPrefixReturnsFullKeys(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertString("app", "root")
	ht.UpsertString("apple", "fruit")
	ht.UpsertString("application", "program")
	ht.UpsertString("banana", "fruit")
	ht.UpsertString("pre\x00one", "one")
	ht.UpsertString("pre\x00two", "two")

	if got, want := ht.KeysWithPrefix("app", true), []string{"app", "apple", "application"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("KeysWithPrefix(app) = %#v, want %#v", got, want)
	}
	if got, want := ht.KeysWithPrefix("pre\x00", true), []string{"pre\x00one", "pre\x00two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("KeysWithPrefix(NUL prefix) = %#v, want %#v", got, want)
	}
	if got := ht.KeysWithPrefix("missing", true); len(got) != 0 {
		t.Fatalf("KeysWithPrefix(missing) = %#v, want empty", got)
	}
}

func TestEntriesWithPrefixPreservesTTLMetadata(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(500, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("cache:a", "a")
	ht.UpsertString("cache:b", "b")
	ht.UpsertString("other", "value")
	if !ht.Expire("cache:a", time.Minute) {
		t.Fatal("Expire(cache:a) = false, want true")
	}

	entries := ht.EntriesWithPrefix("cache:", true)
	if got, want := entryKeys(entries), []string{"cache:a", "cache:b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EntriesWithPrefix keys = %#v, want %#v", got, want)
	}
	values := entriesByKey(entries)
	if got := values["cache:a"]; !got.HasTtl() || !got.IsStringAtRaws() {
		t.Fatalf("cache:a entry = %+v, want string with TTL", got)
	}
	if got := values["cache:b"]; got.HasTtl() || !got.IsStringAtRaws() {
		t.Fatalf("cache:b entry = %+v, want persistent string", got)
	}
}

func TestIterationCleansExpiredKeys(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(600, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("active", "value")
	ht.UpsertString("expired", "value")
	expiredIdx := ht.Get("expired").Index
	if !ht.Expire("expired", time.Second) {
		t.Fatal("Expire(expired) = false, want true")
	}

	now = now.Add(2 * time.Second)
	if got, want := ht.Keys(true), []string{"active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys after expiration = %#v, want %#v", got, want)
	}
	if got := ht.Get("expired"); !got.Empty() {
		t.Fatalf("expired key still exists after iteration: %+v", got)
	}
	if !ht.raws.reusables[expiredIdx] {
		t.Fatalf("expired raw index %d was not reusable", expiredIdx)
	}
}

func TestVacuumExpiredRemovesOnlyExpiredKeys(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(700, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("expired:string", "value")
	expiredRawIdx := ht.Get("expired:string").Index
	ht.UpsertMap("expired:map", Map{"field": "value"})
	expiredMapIdx := ht.Get("expired:map").Index
	ht.UpsertString("active", "value")
	ht.UpsertString("persistent", "value")

	if !ht.Expire("expired:string", time.Second) {
		t.Fatal("Expire(expired:string) = false, want true")
	}
	if !ht.Expire("expired:map", time.Second) {
		t.Fatal("Expire(expired:map) = false, want true")
	}
	if !ht.Expire("active", time.Hour) {
		t.Fatal("Expire(active) = false, want true")
	}

	now = now.Add(2 * time.Second)
	if got := ht.VacuumExpired(); got != 2 {
		t.Fatalf("VacuumExpired() = %d, want 2", got)
	}
	if got := ht.Keys(true); !reflect.DeepEqual(got, []string{"active", "persistent"}) {
		t.Fatalf("keys after vacuum = %#v, want active and persistent", got)
	}
	if got := ht.Get("expired:string"); !got.Empty() {
		t.Fatalf("expired string still exists: %+v", got)
	}
	if got := ht.Get("expired:map"); !got.Empty() {
		t.Fatalf("expired map still exists: %+v", got)
	}
	if !ht.raws.reusables[expiredRawIdx] {
		t.Fatalf("expired raw index %d was not reusable", expiredRawIdx)
	}
	if !ht.maps.reusables[expiredMapIdx] {
		t.Fatalf("expired map index %d was not reusable", expiredMapIdx)
	}
	if got := ht.VacuumExpired(); got != 0 {
		t.Fatalf("second VacuumExpired() = %d, want 0", got)
	}
}

func TestStartExpirationCleanerRemovesExpiredKeysAndStops(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(800, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("key", "value")
	if !ht.Expire("key", time.Second) {
		t.Fatal("Expire(key) = false, want true")
	}
	now = now.Add(2 * time.Second)

	stop := ht.StartExpirationCleaner(time.Millisecond)
	waitUntil(t, 200*time.Millisecond, func() bool {
		return ht.Size() == 0
	})

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expiration cleaner stop did not return")
	}
}

func TestStartExpirationCleanerRejectsInvalidInterval(t *testing.T) {
	ht := newTestTrie(t)

	defer func() {
		if recover() == nil {
			t.Fatal("StartExpirationCleaner(0) did not panic")
		}
	}()
	_ = ht.StartExpirationCleaner(0)
}

func TestConcurrentIterationIsSynchronized(t *testing.T) {
	ht := newTestTrie(t)

	const (
		workers    = 4
		iterations = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := "key:" + strconv.Itoa(worker) + ":" + strconv.Itoa(i)
				ht.UpsertCounter(key, int32(i))
				_ = ht.KeysWithPrefix("key:", true)
				_ = ht.Entries(false)
			}
		}()
	}
	wg.Wait()

	keys := ht.KeysWithPrefix("key:", true)
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("KeysWithPrefix returned unsorted keys: %#v", keys)
	}
	if got, want := len(keys), workers*iterations; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
}

func TestConcurrentTTLOperationsAreSynchronized(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "value")

	const (
		workers    = 4
		iterations = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ht.UpsertString("key", "value")
				_ = ht.Expire("key", time.Minute)
				_ = ht.TTL("key")
				_ = ht.GetString("key")
				_ = ht.Persist("key")
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentOperationsAreSynchronized(t *testing.T) {
	ht := newTestTrie(t)

	const (
		workers    = 6
		iterations = 100
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ht.IncrementCounter("counter", 1)
				ht.AppendString("string", "x")
				ht.UpsertBytes("bytes", []byte{byte(worker), byte(i)})
				ht.PutMap("map", strconv.Itoa(worker)+"-"+strconv.Itoa(i), i)
				ht.PushSlice("slice", worker, i)

				_ = ht.GetCounter("counter")
				_ = ht.GetString("string")
				_ = ht.GetBytes("bytes")
				_ = ht.GetMap("map")
				_ = ht.GetSlice("slice")
			}
		}()
	}
	wg.Wait()

	if got, want := ht.GetCounter("counter"), int32(workers*iterations); got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}
	if got, want := len(ht.GetString("string")), workers*iterations; got != want {
		t.Fatalf("string length = %d, want %d", got, want)
	}
	if got, want := len(ht.GetMap("map")), workers*iterations; got != want {
		t.Fatalf("map length = %d, want %d", got, want)
	}
	if got, want := len(ht.GetSlice("slice")), workers*iterations*2; got != want {
		t.Fatalf("slice length = %d, want %d", got, want)
	}
}

func TestDestroyIsIdempotentAndPreventsUse(t *testing.T) {
	ht := CreateHatTrie()
	ht.UpsertString("key", "value")
	ht.Destroy()
	ht.Destroy()

	defer func() {
		if recover() == nil {
			t.Fatal("Size after Destroy did not panic")
		}
	}()
	_ = ht.Size()
}

func entryKeys(entries []Entry) []string {
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys
}

func entriesByKey(entries []Entry) map[string]HatValue {
	values := make(map[string]HatValue, len(entries))
	for _, entry := range entries {
		values[entry.Key] = entry.Value
	}
	return values
}

func waitUntil(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if ready() {
		return
	}
	t.Fatal("condition was not met before timeout")
}

func cacheStatsEqual(a, b CacheStats) bool {
	return a.Reads == b.Reads &&
		a.Hits == b.Hits &&
		a.Misses == b.Misses &&
		a.Writes == b.Writes &&
		a.Deletes == b.Deletes &&
		a.Expirations == b.Expirations &&
		a.LastHit.Equal(b.LastHit) &&
		a.LastMiss.Equal(b.LastMiss) &&
		a.LastWrite.Equal(b.LastWrite) &&
		a.HitRate == b.HitRate &&
		a.CumulativeHitRate == b.CumulativeHitRate
}

func testPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}
