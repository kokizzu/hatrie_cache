package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
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

func rawIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.raws.array) || ht.raws.reusables.Has(idx)
}

func diskIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.disks.paths) || ht.disks.reusables.Has(idx)
}

func mapIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.maps.array) || ht.maps.reusables.Has(idx)
}

func sliceIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.slices.array) || ht.slices.reusables.Has(idx)
}

func bloomFilterIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.bloomFilters.array) || ht.bloomFilters.reusables.Has(idx)
}

func countMinSketchIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.countMinSketches.array) || ht.countMinSketches.reusables.Has(idx)
}

func hyperLogLogIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.hyperLogLogs.array) || ht.hyperLogLogs.reusables.Has(idx)
}

func topKIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.topKs.array) || ht.topKs.reusables.Has(idx)
}

func cuckooFilterIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.cuckooFilters.array) || ht.cuckooFilters.reusables.Has(idx)
}

func roaringBitmapIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.roaringBitmaps.array) || ht.roaringBitmaps.reusables.Has(idx)
}

func quantileSketchIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.quantileSketches.array) || ht.quantileSketches.reusables.Has(idx)
}

func fenwickTreeIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.fenwickTrees.array) || ht.fenwickTrees.reusables.Has(idx)
}

func sparseBitsetIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.sparseBitsets.array) || ht.sparseBitsets.reusables.Has(idx)
}

func reservoirSampleIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.reservoirSamples.array) || ht.reservoirSamples.reusables.Has(idx)
}

func bloomFilterMissingValue(t *testing.T, ht *HatTrie, key string) string {
	t.Helper()
	for idx := 0; idx < 1000; idx++ {
		candidate := "missing-" + strconv.Itoa(idx)
		if !ht.HasBloomFilter(key, candidate) {
			return candidate
		}
	}
	t.Fatal("could not find a Bloom filter value that reports absent")
	return ""
}

func cuckooFilterMissingValue(t *testing.T, ht *HatTrie, key string) string {
	t.Helper()
	for idx := 0; idx < 1000; idx++ {
		candidate := "missing-" + strconv.Itoa(idx)
		if !ht.HasCuckooFilter(key, candidate) {
			return candidate
		}
	}
	t.Fatal("could not find a Cuckoo filter value that reports absent")
	return ""
}

func bloomFilterFNV64aString(value string) uint64 {
	hash := bloomFilterFNVOffset64
	for idx := 0; idx < len(value); idx++ {
		hash ^= uint64(value[idx])
		hash *= bloomFilterFNVPrime64
	}
	return hash
}

func bloomFilterFNV64String(value string) uint64 {
	hash := bloomFilterFNVOffset64
	for idx := 0; idx < len(value); idx++ {
		hash *= bloomFilterFNVPrime64
		hash ^= uint64(value[idx])
	}
	return hash
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

func TestHatValueStringReportsKnownTypes(t *testing.T) {
	tests := []struct {
		name  string
		value HatValue
		want  string
	}{
		{name: "null", value: HatValue{}, want: "null hat value"},
		{name: "counter", value: HatValue{Index: -7, Flags: DATAVALUE_TYPE_COUNTER}, want: "int32 counter: -7"},
		{name: "bytes", value: HatValue{Index: 1, Flags: DATAVALUE_TYPE_RAW_BYTES}, want: "raw bytes at index: 1"},
		{name: "string", value: HatValue{Index: 2, Flags: DATAVALUE_TYPE_RAW_STRING}, want: "string at index: 2"},
		{name: "map", value: HatValue{Index: 3, Flags: DATAVALUE_TYPE_MAP}, want: "map at index: 3"},
		{name: "slice", value: HatValue{Index: 4, Flags: DATAVALUE_TYPE_SLICE}, want: "slice at index: 4"},
		{name: "leveldb reference", value: HatValue{Index: 5, Flags: DATAVALUE_TYPE_LEVELDB_REF}, want: "leveldb reference at index: 5"},
		{name: "set", value: HatValue{Index: 6, Flags: DATAVALUE_TYPE_SET}, want: "set at index: 6"},
		{name: "priority queue", value: HatValue{Index: 7, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}, want: "priority queue at index: 7"},
		{name: "bloom filter", value: HatValue{Index: 8, Flags: DATAVALUE_TYPE_BLOOM_FILTER}, want: "bloom filter at index: 8"},
		{name: "count-min sketch", value: HatValue{Index: 9, Flags: DATAVALUE_TYPE_COUNT_MIN_SKETCH}, want: "count-min sketch at index: 9"},
		{name: "hyperloglog", value: HatValue{Index: 10, Flags: DATAVALUE_TYPE_HYPERLOGLOG}, want: "hyperloglog at index: 10"},
		{name: "top-k", value: HatValue{Index: 11, Flags: DATAVALUE_TYPE_TOP_K}, want: "top-k at index: 11"},
		{name: "roaring bitmap", value: HatValue{Index: 12, Flags: DATAVALUE_TYPE_ROARING_BITMAP}, want: "roaring bitmap at index: 12"},
		{name: "quantile sketch", value: HatValue{Index: 13, Flags: DATAVALUE_TYPE_QUANTILE_SKETCH}, want: "quantile sketch at index: 13"},
		{name: "fenwick tree", value: HatValue{Index: 14, Flags: DATAVALUE_TYPE_FENWICK_TREE}, want: "fenwick tree at index: 14"},
		{name: "sparse bitset", value: HatValue{Index: 15, Flags: DATAVALUE_TYPE_SPARSE_BITSET}, want: "sparse bitset at index: 15"},
		{name: "reservoir sample", value: HatValue{Index: 16, Flags: DATAVALUE_TYPE_RESERVOIR_SAMPLE}, want: "reservoir sample at index: 16"},
		{name: "xor filter", value: HatValue{Index: 17, Flags: DATAVALUE_TYPE_XOR_FILTER}, want: "xor filter at index: 17"},
		{name: "radix tree", value: HatValue{Index: 18, Flags: DATAVALUE_TYPE_RADIX_TREE}, want: "radix tree at index: 18"},
		{name: "unknown", value: HatValue{Index: 9, Flags: DATAVALUE_TTL_TYPE_BITS}, want: "unknown type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.String(); got != tt.want {
				t.Fatalf("HatValue.String() = %q, want %q", got, tt.want)
			}
		})
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
	if !ht.Delete("nul\x00key") {
		t.Fatal("Delete(NUL-containing key) = false, want true")
	}
	if got := ht.GetString("nul\x00key"); got != "" {
		t.Fatalf("GetString(NUL-containing key after delete) = %q, want empty", got)
	}
}

func TestEmptyKeyIsCountedIterableAndDeletable(t *testing.T) {
	ht := newTestTrie(t)

	if ht.Exists("") {
		t.Fatal("Exists(empty key) before insert = true, want false")
	}

	ht.UpsertString("", "empty")
	hval := ht.Get("")
	if !hval.IsStringAtRaws() {
		t.Fatalf("Get(empty key) = %+v, want string value", hval)
	}
	if got := ht.GetString(""); got != "empty" {
		t.Fatalf("GetString(empty key) = %q, want empty", got)
	}
	if got := ht.Size(); got != 1 {
		t.Fatalf("Size() after empty key insert = %d, want 1", got)
	}
	if got := ht.Keys(true); !reflect.DeepEqual(got, []string{""}) {
		t.Fatalf("Keys() after empty key insert = %#v, want empty key", got)
	}

	if !ht.Delete("") {
		t.Fatal("Delete(empty key) = false, want true")
	}
	if !rawIndexReleased(ht, hval.Index) {
		t.Fatalf("empty key raw index %d was not released", hval.Index)
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("Size() after empty key delete = %d, want 0", got)
	}
	if ht.Exists("") {
		t.Fatal("Exists(empty key) after delete = true, want false")
	}
}

func TestBlankKeysIterateAsDistinctEntries(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "empty")
	ht.UpsertString("\t", "tab")
	ht.UpsertString(" ", "space")

	wantKeys := []string{"", "\t", " "}
	if got := ht.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Keys() = %#v, want %#v", got, wantKeys)
	}
	if got := ht.KeysWithPrefix("", true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("KeysWithPrefix(empty) = %#v, want %#v", got, wantKeys)
	}
	entries := ht.Entries(true)
	if got := entryKeys(entries); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Entries() keys = %#v, want %#v", got, wantKeys)
	}

	values := entriesByKey(entries)
	for key, want := range map[string]string{
		"":   "empty",
		"\t": "tab",
		" ":  "space",
	} {
		if !values[key].IsStringAtRaws() {
			t.Fatalf("entry %q = %+v, want string value", key, values[key])
		}
		if got := ht.GetString(key); got != want {
			t.Fatalf("GetString(%q) = %q, want %q", key, got, want)
		}
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
	if !diskIndexReleased(ht, hval.Index) {
		t.Fatalf("deleted disk index %d was not released", hval.Index)
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
	if !rawIndexReleased(ht, rawIdx) {
		t.Fatalf("replaced raw index %d was not released", rawIdx)
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

func TestCheckedTypedGettersReturnValuesAndCopies(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertCounter("counter", -3)
	ht.UpsertString("string", "value")
	ht.UpsertMap("map", Map{"field": "value"})
	ht.UpsertSlice("slice", Slice{Map{"field": "value"}})
	ht.UpsertSet("set", Set{Map{"field": "value"}})

	if got, ok, err := ht.GetCounterChecked("counter"); err != nil || !ok || got != -3 {
		t.Fatalf("GetCounterChecked(counter) = %d/%v/%v, want -3/true/nil", got, ok, err)
	}
	if got, ok, err := ht.GetStringChecked("string"); err != nil || !ok || got != "value" {
		t.Fatalf("GetStringChecked(string) = %q/%v/%v, want value/true/nil", got, ok, err)
	}
	if got, ok, err := ht.GetStringChecked("counter"); err != nil || !ok || got != "-3" {
		t.Fatalf("GetStringChecked(counter) = %q/%v/%v, want -3/true/nil", got, ok, err)
	}

	gotMap, ok, err := ht.GetMapChecked("map")
	if err != nil || !ok || !reflect.DeepEqual(gotMap, Map{"field": "value"}) {
		t.Fatalf("GetMapChecked(map) = %#v/%v/%v, want stored map", gotMap, ok, err)
	}
	gotMap["field"] = "changed"
	if again, _, _ := ht.GetMapChecked("map"); again["field"] != "value" {
		t.Fatalf("GetMapChecked exposed internal map: %#v", again)
	}

	gotSlice, ok, err := ht.GetSliceChecked("slice")
	if err != nil || !ok || gotSlice[0].(Map)["field"] != "value" {
		t.Fatalf("GetSliceChecked(slice) = %#v/%v/%v, want stored slice", gotSlice, ok, err)
	}
	gotSlice[0].(Map)["field"] = "changed"
	if again, _, _ := ht.GetSliceChecked("slice"); again[0].(Map)["field"] != "value" {
		t.Fatalf("GetSliceChecked exposed internal slice: %#v", again)
	}

	gotSet, ok, err := ht.GetSetChecked("set")
	if err != nil || !ok || gotSet[0].(Map)["field"] != "value" {
		t.Fatalf("GetSetChecked(set) = %#v/%v/%v, want stored set", gotSet, ok, err)
	}
	gotSet[0].(Map)["field"] = "changed"
	if again, _, _ := ht.GetSetChecked("set"); again[0].(Map)["field"] != "value" {
		t.Fatalf("GetSetChecked exposed internal set: %#v", again)
	}

	if _, ok, err := ht.GetCounterChecked("missing"); err != nil || ok {
		t.Fatalf("GetCounterChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}
	if _, ok, err := ht.GetMapChecked("string"); err != nil || ok {
		t.Fatalf("GetMapChecked(wrong type) ok/error = %v/%v, want false/nil", ok, err)
	}
}

func TestCheckedMapAndSliceOperationsReturnValuesAndCopies(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertMap("map", Map{
		"nested": Map{"field": "value"},
		"remove": "value",
	})
	ht.UpsertSlice("slice", Slice{Map{"field": "first"}, "second", "third"})

	peeked, ok, err := ht.PeekMapChecked("map", "nested")
	if err != nil || !ok || peeked.(Map)["field"] != "value" {
		t.Fatalf("PeekMapChecked(nested) = %#v/%v/%v, want stored map", peeked, ok, err)
	}
	peeked.(Map)["field"] = "changed"
	if again, _, _ := ht.PeekMapChecked("map", "nested"); again.(Map)["field"] != "value" {
		t.Fatalf("PeekMapChecked exposed internal map: %#v", again)
	}

	taken, ok, err := ht.TakeMapChecked("map", "remove")
	if err != nil || !ok || taken != "value" {
		t.Fatalf("TakeMapChecked(remove) = %#v/%v/%v, want value/true/nil", taken, ok, err)
	}
	if _, ok, err := ht.PeekMapChecked("map", "remove"); err != nil || ok {
		t.Fatalf("PeekMapChecked(after take) ok/error = %v/%v, want false/nil", ok, err)
	}
	if _, ok, err := ht.PeekMapChecked("missing", "field"); err != nil || ok {
		t.Fatalf("PeekMapChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}

	head, ok, err := ht.HeadSliceChecked("slice")
	if err != nil || !ok || head.(Map)["field"] != "first" {
		t.Fatalf("HeadSliceChecked(slice) = %#v/%v/%v, want first map", head, ok, err)
	}
	head.(Map)["field"] = "changed"
	if again, _, _ := ht.HeadSliceChecked("slice"); again.(Map)["field"] != "first" {
		t.Fatalf("HeadSliceChecked exposed internal slice value: %#v", again)
	}
	if tail, ok, err := ht.TailSliceChecked("slice"); err != nil || !ok || tail != "third" {
		t.Fatalf("TailSliceChecked(slice) = %#v/%v/%v, want third/true/nil", tail, ok, err)
	}
	if popped, ok, err := ht.PopSliceChecked("slice"); err != nil || !ok || popped != "third" {
		t.Fatalf("PopSliceChecked(slice) = %#v/%v/%v, want third/true/nil", popped, ok, err)
	}
	if shifted, ok, err := ht.ShiftSliceChecked("slice"); err != nil || !ok || shifted.(Map)["field"] != "first" {
		t.Fatalf("ShiftSliceChecked(slice) = %#v/%v/%v, want first map", shifted, ok, err)
	}
	if got := ht.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"second"}) {
		t.Fatalf("GetSlice(after pop/shift) = %#v, want second only", got)
	}
	if _, ok, err := ht.PopSliceChecked("missing"); err != nil || ok {
		t.Fatalf("PopSliceChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}
}

func TestBytesCheckedDiskWriteFailureDoesNotMutate(t *testing.T) {
	ht := newTestTrie(t)
	largeValue := testPayload(DiskBytesThreshold + 1)
	blockedPath := ht.disks.pathFor(0)

	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}
	if err := ht.UpsertBytesChecked("large", largeValue); err == nil {
		t.Fatal("UpsertBytesChecked(missing large) error = nil, want write error")
	}
	if got := ht.Get("large"); !got.Empty() {
		t.Fatalf("failed UpsertBytesChecked created value %+v", got)
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed insert = %d, want 0", got)
	}
	if err := os.Remove(blockedPath); err != nil {
		t.Fatalf("Remove(blocked path) error = %v", err)
	}

	ht.UpsertString("string", "keep")
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked replacement path) error = %v", err)
	}
	if err := ht.UpsertBytesChecked("string", largeValue); err == nil {
		t.Fatal("UpsertBytesChecked(replace string) error = nil, want write error")
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("failed UpsertBytesChecked replaced string with %q, want keep", got)
	}
	if err := os.Remove(blockedPath); err != nil {
		t.Fatalf("Remove(blocked replacement path) error = %v", err)
	}

	smallValue := []byte("small")
	if err := ht.UpsertBytesChecked("bytes", smallValue); err != nil {
		t.Fatalf("UpsertBytesChecked(small) error = %v", err)
	}
	rawValue := ht.Get("bytes")
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked raw replacement path) error = %v", err)
	}
	if err := ht.UpsertBytesChecked("bytes", largeValue); err == nil {
		t.Fatal("UpsertBytesChecked(replace raw bytes) error = nil, want write error")
	}
	if got := ht.Get("bytes"); got != rawValue {
		t.Fatalf("failed UpsertBytesChecked changed metadata %+v, want %+v", got, rawValue)
	}
	if got := ht.GetBytes("bytes"); !bytes.Equal(got, smallValue) {
		t.Fatalf("failed UpsertBytesChecked changed bytes %q, want %q", got, smallValue)
	}
}

func TestGetBytesCheckedReturnsDiskReadError(t *testing.T) {
	ht := newTestTrie(t)
	payload := testPayload(DiskBytesThreshold + 1)

	if err := ht.UpsertBytesChecked("large", payload); err != nil {
		t.Fatalf("UpsertBytesChecked(large) error = %v", err)
	}
	hval := ht.Get("large")
	path := ht.disks.paths[hval.Index]
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(disk value) error = %v", err)
	}

	if got, err := ht.GetBytesChecked("large"); err == nil || got != nil {
		t.Fatalf("GetBytesChecked(missing disk file) = %q/%v, want nil/error", got, err)
	}
	if got := ht.GetBytes("large"); got != nil {
		t.Fatalf("legacy GetBytes(missing disk file) = %q, want nil", got)
	}
}

func TestApplySnapshotBytesWriteFailureKeepsExistingValue(t *testing.T) {
	ht := newTestTrie(t)
	original := []byte("small")
	largeValue := testPayload(DiskBytesThreshold + 1)

	if err := ht.UpsertBytesChecked("key", original); err != nil {
		t.Fatalf("UpsertBytesChecked(original) error = %v", err)
	}
	originalValue := ht.Get("key")
	blockedPath := ht.disks.pathFor(0)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	ht.mu.Lock()
	_, err := ht.applySnapshotOperationLocked(snapshotOperation{
		entry: snapshotEntry{Key: "key", Type: "bytes"},
		bytes: largeValue,
	})
	ht.mu.Unlock()
	if err == nil {
		t.Fatal("applySnapshotOperationLocked(bytes write failure) error = nil, want error")
	}
	if got := ht.Get("key"); got != originalValue {
		t.Fatalf("failed snapshot apply changed metadata %+v, want %+v", got, originalValue)
	}
	if got := ht.GetBytes("key"); !bytes.Equal(got, original) {
		t.Fatalf("failed snapshot apply changed bytes %q, want %q", got, original)
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

func TestTakeMapLastEntryKeepsEmptyMapReadable(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertMap("map", Map{"only": "value"})

	if got := ht.TakeMap("map", "only"); got != "value" {
		t.Fatalf("TakeMap(only) = %v, want value", got)
	}
	value := ht.GetMap("map")
	if value == nil || len(value) != 0 {
		t.Fatalf("GetMap(after taking last entry) = %#v, want empty map", value)
	}
	if !ht.Get("map").IsMap() {
		t.Fatal("taking last map entry removed map key")
	}
}

func TestMapStorageCompactsSparseBackingMap(t *testing.T) {
	store := CreateMapStorage()
	value := make(Map, 96)
	for idx := 0; idx < 96; idx++ {
		value[strconv.Itoa(idx)] = idx
	}
	mapIdx := store.Add(value)

	for idx := 0; idx < 48; idx++ {
		key := strconv.Itoa(idx)
		got, ok := store.TakeEntry(mapIdx, key)
		if !ok || got != idx {
			t.Fatalf("TakeEntry(%s) = %v/%v, want %d/true", key, got, ok, idx)
		}
	}
	if store.deleted[mapIdx] != 0 {
		t.Fatalf("deleted count after sparse compaction = %d, want 0", store.deleted[mapIdx])
	}
	if got := len(store.array[mapIdx]); got != 48 {
		t.Fatalf("map len after removals = %d, want 48", got)
	}
	for idx := 48; idx < 96; idx++ {
		key := strconv.Itoa(idx)
		if got := store.array[mapIdx][key]; got != idx {
			t.Fatalf("remaining map entry %s = %v, want %d", key, got, idx)
		}
	}
}

func TestMapStoragePutEntriesClonesValues(t *testing.T) {
	store := CreateMapStorage()
	idx := store.Add(Map{"existing": "value"})
	nested := Map{"field": "stored"}
	payload := []byte("bytes")

	store.PutEntries(idx, Map{"nested": nested, "payload": payload})
	nested["field"] = "caller"
	payload[0] = 'X'

	got := store.array[idx]
	if got["existing"] != "value" {
		t.Fatalf("existing map entry = %v, want value", got["existing"])
	}
	if got["nested"].(Map)["field"] != "stored" {
		t.Fatalf("nested map entry = %#v, want stored clone", got["nested"])
	}
	if string(got["payload"].([]byte)) != "bytes" {
		t.Fatalf("payload map entry = %q, want bytes", got["payload"])
	}
}

func TestMapOperationsDeepCopyNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	nested := Map{"name": "ivi"}
	items := Slice{"one", Map{"two": true}}

	ht.UpsertMap("map", Map{"nested": nested, "items": items})
	nested["name"] = "changed"
	items[0] = "changed"
	items[1].(Map)["two"] = false

	got := ht.GetMap("map")
	if got["nested"].(Map)["name"] != "ivi" || got["items"].(Slice)[0] != "one" || got["items"].(Slice)[1].(Map)["two"] != true {
		t.Fatalf("stored map changed through caller input: %#v", got)
	}

	got["nested"].(Map)["name"] = "from-get"
	got["items"].(Slice)[1].(Map)["two"] = false
	if again := ht.GetMap("map"); again["nested"].(Map)["name"] != "ivi" || again["items"].(Slice)[1].(Map)["two"] != true {
		t.Fatalf("GetMap exposed nested values: %#v", again)
	}

	peek := ht.PeekMap("map", "nested").(Map)
	peek["name"] = "from-peek"
	if again := ht.PeekMap("map", "nested").(Map); again["name"] != "ivi" {
		t.Fatalf("PeekMap exposed nested value: %#v", again)
	}

	patch := Map{"name": "put"}
	ht.PutMap("map", "patch", patch)
	patch["name"] = "caller"
	if got := ht.PeekMap("map", "patch").(Map); got["name"] != "put" {
		t.Fatalf("PutMap stored caller-owned nested value: %#v", got)
	}
	ht.PeekMap("map", "patch").(Map)["name"] = "from-peek"
	if again := ht.PeekMap("map", "patch").(Map); again["name"] != "put" {
		t.Fatalf("PeekMap exposed PutMap nested value: %#v", again)
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

func TestKeyStatsTrackExistingKeyAccessAndAvoidUnknownMissGrowth(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(950, 0)
	ht.now = func() time.Time { return now }

	if got := ht.GetString("missing"); got != "" {
		t.Fatalf("GetString(missing) = %q, want empty", got)
	}
	if stats, ok := ht.StatsForKey("missing"); ok {
		t.Fatalf("StatsForKey(missing) = %#v, true; want false", stats)
	}

	ht.UpsertString("key", "value")
	writeAt := now
	now = now.Add(time.Second)
	if got := ht.GetString("key"); got != "value" {
		t.Fatalf("GetString(key) = %q, want value", got)
	}
	hitAt := now
	now = now.Add(time.Second)
	if got := ht.GetCounter("key"); got != 0 {
		t.Fatalf("GetCounter(string key) = %d, want 0", got)
	}
	missAt := now

	stats, ok := ht.StatsForKey("key")
	if !ok {
		t.Fatal("StatsForKey(key) = false, want true")
	}
	if stats.Reads != 2 || stats.Hits != 1 || stats.Misses != 1 || stats.Writes != 1 {
		t.Fatalf("key stats counters = %#v, want 2 reads, 1 hit, 1 miss, 1 write", stats)
	}
	if stats.HitRate != 0.5 || stats.CumulativeHitRate != 0.5 {
		t.Fatalf("key hit rates = %f/%f, want 0.5/0.5", stats.HitRate, stats.CumulativeHitRate)
	}
	if !stats.LastWrite.Equal(writeAt) || !stats.LastHit.Equal(hitAt) || !stats.LastMiss.Equal(missAt) {
		t.Fatalf("key stats times = %#v, want write %s hit %s miss %s", stats, writeAt, hitAt, missAt)
	}

	if !ht.Delete("key") {
		t.Fatal("Delete(key) = false, want true")
	}
	if stats, ok := ht.StatsForKey("key"); ok {
		t.Fatalf("StatsForKey(deleted key) = %#v, true; want false", stats)
	}
}

func TestRestoreKeyStatsUpdatesRatesAndClearsStats(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "value")

	restored := KeyStats{
		Reads:  4,
		Hits:   3,
		Misses: 1,
		Writes: 2,
	}
	ht.restoreKeyStats("key", &restored)
	got, ok := ht.StatsForKey("key")
	if !ok {
		t.Fatal("StatsForKey(restored key) = false, want true")
	}
	if got.Reads != 4 || got.Hits != 3 || got.Misses != 1 || got.Writes != 2 {
		t.Fatalf("restored key stats = %#v, want restored counters", got)
	}
	if got.HitRate != 0.75 || got.CumulativeHitRate != 0.75 {
		t.Fatalf("restored hit rates = %f/%f, want 0.75/0.75", got.HitRate, got.CumulativeHitRate)
	}

	ht.restoreKeyStats("key", nil)
	if stats, ok := ht.StatsForKey("key"); ok {
		t.Fatalf("StatsForKey(after clear) = %#v, true; want false", stats)
	}

	ht.restoreKeyStats("missing", &restored)
	if stats, ok := ht.StatsForKey("missing"); ok {
		t.Fatalf("StatsForKey(missing restored key) = %#v, true; want false", stats)
	}
}

func TestKeyStatsTrackEmptyKeyAccessAndAvoidUnknownMissGrowth(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(975, 0)
	ht.now = func() time.Time { return now }

	if got := ht.GetString(""); got != "" {
		t.Fatalf("GetString(empty missing key) = %q, want empty", got)
	}
	if stats, ok := ht.StatsForKey(""); ok {
		t.Fatalf("StatsForKey(empty missing key) = %#v, true; want false", stats)
	}

	ht.UpsertString("", "value")
	writeAt := now
	now = now.Add(time.Second)
	if got := ht.GetString(""); got != "value" {
		t.Fatalf("GetString(empty key) = %q, want value", got)
	}
	hitAt := now

	stats, ok := ht.StatsForKey("")
	if !ok {
		t.Fatal("StatsForKey(empty key) = false, want true")
	}
	if stats.Writes != 1 || stats.Reads != 1 || stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("empty key stats = %+v, want writes 1 reads/hits 1 misses 0", stats)
	}
	if !stats.LastWrite.Equal(writeAt) || !stats.LastHit.Equal(hitAt) {
		t.Fatalf("empty key timestamps = write %s hit %s, want %s/%s", stats.LastWrite, stats.LastHit, writeAt, hitAt)
	}

	if !ht.Delete("") {
		t.Fatal("Delete(empty key) = false, want true")
	}
	if stats, ok := ht.StatsForKey(""); ok {
		t.Fatalf("StatsForKey(deleted empty key) = %#v, true; want false", stats)
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

func TestSliceOperationsDeepCopyNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	item := Map{"field": "value"}

	ht.PushSlice("slice", item)
	item["field"] = "caller"
	head := ht.HeadSlice("slice").(Map)
	if head["field"] != "value" {
		t.Fatalf("HeadSlice() = %#v, want stored value", head)
	}
	head["field"] = "head"
	if again := ht.HeadSlice("slice").(Map); again["field"] != "value" {
		t.Fatalf("HeadSlice exposed nested value: %#v", again)
	}

	values := ht.GetSlice("slice")
	values[0].(Map)["field"] = "get"
	if again := ht.GetSlice("slice"); again[0].(Map)["field"] != "value" {
		t.Fatalf("GetSlice exposed nested value: %#v", again)
	}
}

func TestUpsertSliceReplacesExistingSliceAndClearsTTL(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("slice", Slice{"old", "values"})
	idx := ht.Get("slice").Index
	if !ht.Expire("slice", time.Minute) {
		t.Fatal("Expire(slice) = false, want true")
	}

	ht.UpsertSlice("slice", Slice{"new"})
	if got := ht.Get("slice"); !got.IsSlice() || got.Index != idx || got.HasTtl() {
		t.Fatalf("slice value after UpsertSlice = %+v, want same slice index without TTL", got)
	}
	if got := ht.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"new"}) {
		t.Fatalf("GetSlice(after UpsertSlice) = %#v, want new value", got)
	}
	if ttl := ht.TTL("slice"); ttl != NoTTL {
		t.Fatalf("TTL(slice) = %s, want NoTTL", ttl)
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

func TestSetOperations(t *testing.T) {
	ht := newTestTrie(t)

	if added := ht.AddSet("set", "go", "cache", "go", json.Number("2")); added != 3 {
		t.Fatalf("AddSet() = %d, want 3", added)
	}
	if hval := ht.Get("set"); !hval.IsSet() {
		t.Fatalf("AddSet on missing key stored type %+v, want set", hval)
	}
	if !ht.HasSet("set", "go") {
		t.Fatal("HasSet(go) = false, want true")
	}
	if ht.HasSet("set", "missing") {
		t.Fatal("HasSet(missing) = true, want false")
	}
	if removed := ht.RemoveSet("set", "go", "missing"); removed != 1 {
		t.Fatalf("RemoveSet() = %d, want 1", removed)
	}
	if ht.HasSet("set", "go") {
		t.Fatal("HasSet(go) after remove = true, want false")
	}
	if got := ht.GetSet("set"); !reflect.DeepEqual(got, Set{"cache", json.Number("2")}) {
		t.Fatalf("GetSet() = %#v, want deterministic values", got)
	}

	copied := ht.GetSet("set")
	copied[0] = "changed"
	if got := ht.GetSet("set"); !reflect.DeepEqual(got, Set{"cache", json.Number("2")}) {
		t.Fatalf("GetSet exposed internal set: got %#v", got)
	}

	ht.UpsertSet("set", Set{"new", "new"})
	if got := ht.GetSet("set"); !reflect.DeepEqual(got, Set{"new"}) {
		t.Fatalf("UpsertSet dedupe result = %#v, want new", got)
	}
}

func TestSetRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	added, err := ht.AddSetChecked("set", "go")
	if err != nil || added != 1 {
		t.Fatalf("AddSetChecked(go) = %d, %v; want 1, nil", added, err)
	}

	if added, err := ht.AddSetChecked("set", "cache", unsupported); err == nil || added != 0 {
		t.Fatalf("AddSetChecked(with unsupported) = %d, %v; want 0, error", added, err)
	}
	if got := ht.GetSet("set"); !reflect.DeepEqual(got, Set{"go"}) {
		t.Fatalf("GetSet(after rejected add) = %#v, want original set", got)
	}

	if removed, err := ht.RemoveSetChecked("set", "go", unsupported); err == nil || removed != 0 {
		t.Fatalf("RemoveSetChecked(with unsupported) = %d, %v; want 0, error", removed, err)
	}
	if !ht.HasSet("set", "go") {
		t.Fatal("RemoveSetChecked removed a value before returning an error")
	}

	if added, err := ht.AddSetChecked("missing", unsupported); err == nil || added != 0 {
		t.Fatalf("AddSetChecked(missing unsupported) = %d, %v; want 0, error", added, err)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("AddSetChecked(missing unsupported) created value %+v", got)
	}

	ht.UpsertString("string", "keep")
	if added, err := ht.AddSetChecked("string", unsupported); err == nil || added != 0 {
		t.Fatalf("AddSetChecked(replace unsupported) = %d, %v; want 0, error", added, err)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("AddSetChecked(replace unsupported) mutated value %q, want keep", got)
	}

	if err := ht.UpsertSetChecked("set", Set{"new", unsupported}); err == nil {
		t.Fatal("UpsertSetChecked(with unsupported) = nil, want error")
	}
	if got := ht.GetSet("set"); !reflect.DeepEqual(got, Set{"go"}) {
		t.Fatalf("GetSet(after rejected upsert) = %#v, want original set", got)
	}

	if hit, err := ht.HasSetChecked("set", unsupported); err == nil || hit {
		t.Fatalf("HasSetChecked(unsupported) = %t, %v; want false, error", hit, err)
	}
	if added := ht.AddSet("legacy", unsupported); added != 0 {
		t.Fatalf("legacy AddSet(unsupported) = %d, want 0", added)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy AddSet(unsupported) created value %+v", got)
	}
	if removed := ht.RemoveSet("set", unsupported); removed != 0 {
		t.Fatalf("legacy RemoveSet(unsupported) = %d, want 0", removed)
	}
	if ht.HasSet("set", unsupported) {
		t.Fatal("legacy HasSet(unsupported) = true, want false")
	}
}

func TestSetRemoveLastValueKeepsEmptySetReadable(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSet("set", Set{"only"})

	if removed := ht.RemoveSet("set", "only"); removed != 1 {
		t.Fatalf("RemoveSet(only) = %d, want 1", removed)
	}
	got := ht.GetSet("set")
	if got == nil || len(got) != 0 {
		t.Fatalf("GetSet(after removing last value) = %#v, want empty set", got)
	}
	if !ht.Get("set").IsSet() {
		t.Fatal("removing last set value removed set key")
	}
}

func TestSetDataCompactsSparseBackingMap(t *testing.T) {
	var set setData
	values := make(Set, 96)
	for idx := range values {
		values[idx] = idx
	}
	if added := set.Add(values...); added != len(values) {
		t.Fatalf("set.Add(values) = %d, want %d", added, len(values))
	}

	for idx := 0; idx < 48; idx++ {
		if removed := set.Remove(idx); removed != 1 {
			t.Fatalf("set.Remove(%d) = %d, want 1", idx, removed)
		}
	}
	if set.deleted != 0 {
		t.Fatalf("set.deleted after sparse compaction = %d, want 0", set.deleted)
	}
	if got := set.Len(); got != 48 {
		t.Fatalf("set.Len() after removals = %d, want 48", got)
	}
	for idx := 48; idx < 96; idx++ {
		if !set.Has(idx) {
			t.Fatalf("set.Has(%d) = false, want true", idx)
		}
	}
}

func TestSetOperationsDeepCopyNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	item := Map{"field": "value"}

	if added := ht.AddSet("set", item); added != 1 {
		t.Fatalf("AddSet() = %d, want 1", added)
	}
	item["field"] = "caller"
	if !ht.HasSet("set", Map{"field": "value"}) {
		t.Fatal("HasSet(original nested map) = false, want true")
	}

	values := ht.GetSet("set")
	if values[0].(Map)["field"] != "value" {
		t.Fatalf("GetSet() = %#v, want stored value", values)
	}
	values[0].(Map)["field"] = "get"
	if again := ht.GetSet("set"); again[0].(Map)["field"] != "value" {
		t.Fatalf("GetSet exposed nested value: %#v", again)
	}
}

func TestCompositeValuesDeepCopyBytesAndPriorityQueues(t *testing.T) {
	ht := newTestTrie(t)
	payload := []byte("value")
	queue := PriorityQueue{{Priority: 1, Value: Map{"job": "build"}}}

	ht.UpsertMap("map", Map{"bytes": payload, "queue": queue})
	ht.PushSlice("slice", payload, queue)
	ht.AddSet("set", payload, queue)

	payload[0] = 'X'
	queue[0].Value.(Map)["job"] = "caller"
	assertCompositeValuesDeepCopied(t, ht)

	gotMap := ht.GetMap("map")
	gotMap["bytes"].([]byte)[0] = 'M'
	gotMap["queue"].(PriorityQueue)[0].Value.(Map)["job"] = "map-read"

	gotSlice := ht.GetSlice("slice")
	gotSlice[0].([]byte)[0] = 'S'
	gotSlice[1].(PriorityQueue)[0].Value.(Map)["job"] = "slice-read"

	gotSet := ht.GetSet("set")
	for _, value := range gotSet {
		switch typed := value.(type) {
		case []byte:
			typed[0] = 'T'
		case PriorityQueue:
			typed[0].Value.(Map)["job"] = "set-read"
		}
	}
	assertCompositeValuesDeepCopied(t, ht)
}

func assertCompositeValuesDeepCopied(t *testing.T, ht *HatTrie) {
	t.Helper()

	gotMap := ht.GetMap("map")
	if got := string(gotMap["bytes"].([]byte)); got != "value" {
		t.Fatalf("map nested bytes = %q, want value", got)
	}
	if got := gotMap["queue"].(PriorityQueue)[0].Value.(Map)["job"]; got != "build" {
		t.Fatalf("map nested priority queue job = %v, want build", got)
	}

	gotSlice := ht.GetSlice("slice")
	if got := string(gotSlice[0].([]byte)); got != "value" {
		t.Fatalf("slice nested bytes = %q, want value", got)
	}
	if got := gotSlice[1].(PriorityQueue)[0].Value.(Map)["job"]; got != "build" {
		t.Fatalf("slice nested priority queue job = %v, want build", got)
	}

	var sawBytes, sawQueue bool
	for _, value := range ht.GetSet("set") {
		switch typed := value.(type) {
		case []byte:
			sawBytes = true
			if got := string(typed); got != "value" {
				t.Fatalf("set nested bytes = %q, want value", got)
			}
		case PriorityQueue:
			sawQueue = true
			if got := typed[0].Value.(Map)["job"]; got != "build" {
				t.Fatalf("set nested priority queue job = %v, want build", got)
			}
		}
	}
	if !sawBytes || !sawQueue {
		t.Fatalf("set values saw bytes=%v queue=%v, want both", sawBytes, sawQueue)
	}
}

func TestPriorityQueueOperations(t *testing.T) {
	ht := newTestTrie(t)

	if added := ht.PushPriorityQueue("queue", 10, "slow"); added != 1 {
		t.Fatalf("PushPriorityQueue(slow) = %d, want 1", added)
	}
	if added := ht.PushPriorityQueue("queue", 1, "urgent", "also-urgent"); added != 2 {
		t.Fatalf("PushPriorityQueue(urgent) = %d, want 2", added)
	}
	if hval := ht.Get("queue"); !hval.IsPriorityQueue() {
		t.Fatalf("PushPriorityQueue on missing key stored type %+v, want priority queue", hval)
	}
	if got, ok := ht.PeekPriorityQueue("queue"); !ok || got.Priority != 1 || got.Value != "urgent" {
		t.Fatalf("PeekPriorityQueue() = %#v/%v, want urgent priority 1", got, ok)
	}
	if got, ok := ht.PopPriorityQueue("queue"); !ok || got.Priority != 1 || got.Value != "urgent" {
		t.Fatalf("first PopPriorityQueue() = %#v/%v, want urgent priority 1", got, ok)
	}
	if got, ok := ht.PopPriorityQueue("queue"); !ok || got.Priority != 1 || got.Value != "also-urgent" {
		t.Fatalf("second PopPriorityQueue() = %#v/%v, want FIFO tie", got, ok)
	}
	if got := ht.GetPriorityQueue("queue"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 10, Value: "slow"}}) {
		t.Fatalf("GetPriorityQueue() = %#v, want slow remaining", got)
	}

	copied := ht.GetPriorityQueue("queue")
	copied[0].Value = "changed"
	if got := ht.GetPriorityQueue("queue"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 10, Value: "slow"}}) {
		t.Fatalf("GetPriorityQueue exposed internal queue: got %#v", got)
	}

	ht.UpsertPriorityQueue("queue", PriorityQueue{{Priority: -1, Value: "first"}, {Priority: 5, Value: "later"}})
	if got, ok := ht.PopPriorityQueue("queue"); !ok || got.Value != "first" {
		t.Fatalf("PopPriorityQueue after upsert = %#v/%v, want first", got, ok)
	}
}

func TestPriorityQueueBulkUpsertPreservesTieOrderBeforeLaterPush(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertPriorityQueue("queue", PriorityQueue{
		{Priority: 5, Value: "slow"},
		{Priority: 1, Value: "first"},
		{Priority: 1, Value: "second"},
	})

	if added := ht.PushPriorityQueue("queue", 1, "third"); added != 1 {
		t.Fatalf("PushPriorityQueue(third) = %d, want 1", added)
	}
	for _, want := range []string{"first", "second", "third", "slow"} {
		got, ok := ht.PopPriorityQueue("queue")
		if !ok || got.Value != want {
			t.Fatalf("PopPriorityQueue() = %#v/%v, want %q", got, ok, want)
		}
	}
}

func TestPriorityQueueBulkPushMaintainsFIFOAcrossGrowth(t *testing.T) {
	ht := newTestTrie(t)
	const count = 65
	values := make([]interface{}, 0, count-1)
	for idx := 1; idx < count; idx++ {
		values = append(values, "job-"+strconv.Itoa(idx))
	}

	if added := ht.PushPriorityQueue("queue", 1, "job-0", values...); added != count {
		t.Fatalf("PushPriorityQueue(batch) = %d, want %d", added, count)
	}
	for idx := 0; idx < count; idx++ {
		want := "job-" + strconv.Itoa(idx)
		got, ok := ht.PopPriorityQueue("queue")
		if !ok || got.Value != want {
			t.Fatalf("PopPriorityQueue(%d) = %#v/%v, want %q", idx, got, ok, want)
		}
	}
}

func TestPriorityQueueOperationsDeepCopyNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	item := Map{"job": "build"}

	if added := ht.PushPriorityQueue("queue", 1, item); added != 1 {
		t.Fatalf("PushPriorityQueue() = %d, want 1", added)
	}
	item["job"] = "caller"
	peek, ok := ht.PeekPriorityQueue("queue")
	if !ok || peek.Value.(Map)["job"] != "build" {
		t.Fatalf("PeekPriorityQueue() = %#v/%v, want stored value", peek, ok)
	}
	peek.Value.(Map)["job"] = "peek"
	if again, ok := ht.PeekPriorityQueue("queue"); !ok || again.Value.(Map)["job"] != "build" {
		t.Fatalf("PeekPriorityQueue exposed nested value: %#v/%v", again, ok)
	}

	items := ht.GetPriorityQueue("queue")
	items[0].Value.(Map)["job"] = "get"
	if again, ok := ht.PeekPriorityQueue("queue"); !ok || again.Value.(Map)["job"] != "build" {
		t.Fatalf("GetPriorityQueue exposed nested value: %#v/%v", again, ok)
	}
}

func TestCheckedPriorityQueueOperationsReturnValuesAndCopies(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertPriorityQueue("queue", PriorityQueue{
		{Priority: 5, Value: Map{"job": "old"}},
		{Priority: 1, Value: "new"},
	})

	peek, ok, err := ht.PeekPriorityQueueChecked("queue")
	if err != nil || !ok || peek.Priority != 1 || peek.Value != "new" {
		t.Fatalf("PeekPriorityQueueChecked(queue) = %#v/%v/%v, want new priority 1", peek, ok, err)
	}

	items, ok, err := ht.GetPriorityQueueChecked("queue")
	if err != nil || !ok || !reflect.DeepEqual(items, PriorityQueue{
		{Priority: 1, Value: "new"},
		{Priority: 5, Value: Map{"job": "old"}},
	}) {
		t.Fatalf("GetPriorityQueueChecked(queue) = %#v/%v/%v, want priority order", items, ok, err)
	}
	items[1].Value.(Map)["job"] = "changed"
	if again, _, _ := ht.GetPriorityQueueChecked("queue"); again[1].Value.(Map)["job"] != "old" {
		t.Fatalf("GetPriorityQueueChecked exposed nested value: %#v", again)
	}

	popped, ok, err := ht.PopPriorityQueueChecked("queue")
	if err != nil || !ok || popped.Priority != 1 || popped.Value != "new" {
		t.Fatalf("PopPriorityQueueChecked(queue) = %#v/%v/%v, want new priority 1", popped, ok, err)
	}
	next, ok, err := ht.PeekPriorityQueueChecked("queue")
	if err != nil || !ok || next.Priority != 5 || next.Value.(Map)["job"] != "old" {
		t.Fatalf("PeekPriorityQueueChecked(after pop) = %#v/%v/%v, want old priority 5", next, ok, err)
	}
	next.Value.(Map)["job"] = "peek"
	if again, _, _ := ht.PeekPriorityQueueChecked("queue"); again.Value.(Map)["job"] != "old" {
		t.Fatalf("PeekPriorityQueueChecked exposed nested value: %#v", again)
	}

	ht.UpsertString("string", "value")
	if _, ok, err := ht.GetPriorityQueueChecked("missing"); err != nil || ok {
		t.Fatalf("GetPriorityQueueChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}
	if _, ok, err := ht.PeekPriorityQueueChecked("string"); err != nil || ok {
		t.Fatalf("PeekPriorityQueueChecked(wrong type) ok/error = %v/%v, want false/nil", ok, err)
	}
	if _, ok, err := ht.PopPriorityQueueChecked("string"); err != nil || ok {
		t.Fatalf("PopPriorityQueueChecked(wrong type) ok/error = %v/%v, want false/nil", ok, err)
	}
}

func TestBloomFilterOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertBloomFilter("seen", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	if hval := ht.Get("seen"); !hval.IsBloomFilter() {
		t.Fatalf("UpsertBloomFilter stored type %+v, want bloom filter", hval)
	}
	if added := ht.AddBloomFilter("seen", "alpha", "beta", "alpha"); added != 2 {
		t.Fatalf("AddBloomFilter() = %d, want 2 new values", added)
	}
	if !ht.HasBloomFilter("seen", "alpha") || !ht.HasBloomFilter("seen", "beta") {
		t.Fatal("HasBloomFilter(inserted values) = false, want true")
	}
	if missing := bloomFilterMissingValue(t, ht, "seen"); ht.HasBloomFilter("seen", missing) {
		t.Fatal("HasBloomFilter(missing) = true, want false")
	}
	info, ok := ht.BloomFilterInfo("seen")
	if !ok {
		t.Fatal("BloomFilterInfo(seen) = false, want true")
	}
	if info.BitCount < minBloomFilterBits || info.HashCount == 0 || info.Insertions != 2 || info.SetBits == 0 {
		t.Fatalf("BloomFilterInfo(seen) = %#v, want populated compact filter", info)
	}

	if err := ht.UpsertBloomFilter("seen", 10, 0.1); err != nil {
		t.Fatalf("UpsertBloomFilter(replace) error = %v", err)
	}
	if ht.HasBloomFilter("seen", "alpha") {
		t.Fatal("replaced Bloom filter still contains old value")
	}

	if added := ht.AddBloomFilter("auto", "value"); added != 1 {
		t.Fatalf("AddBloomFilter(auto) = %d, want 1", added)
	}
	if hval := ht.Get("auto"); !hval.IsBloomFilter() {
		t.Fatalf("AddBloomFilter(auto) stored type %+v, want bloom filter", hval)
	}
}

func TestBloomFilterHashesJSONRepresentationForCompatibility(t *testing.T) {
	key := []byte(`"alpha"`)
	if bloomFilterFNV64a(key) != bloomFilterFNV64aString(string(key)) {
		t.Fatal("bloomFilterFNV64a changed from JSON string hash representation")
	}
	if bloomFilterFNV64(key) != bloomFilterFNV64String(string(key)) {
		t.Fatal("bloomFilterFNV64 changed from JSON string hash representation")
	}
}

func TestBloomFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if added, err := ht.AddBloomFilterChecked("seen", "alpha"); err != nil || added != 1 {
		t.Fatalf("AddBloomFilterChecked(alpha) = %d/%v, want 1/nil", added, err)
	}

	if added, err := ht.AddBloomFilterChecked("seen", "beta", func() {}); err == nil {
		t.Fatalf("AddBloomFilterChecked(unsupported batch) = %d/nil, want error", added)
	}
	info, ok := ht.BloomFilterInfo("seen")
	if !ok || info.Insertions != 1 {
		t.Fatalf("BloomFilterInfo(after rejected batch) = %#v/%v, want one insertion", info, ok)
	}
	if !ht.HasBloomFilter("seen", "alpha") {
		t.Fatal("rejected batch removed existing bloom filter value")
	}

	if added, err := ht.AddBloomFilterChecked("missing", func() {}); err == nil {
		t.Fatalf("AddBloomFilterChecked(missing unsupported) = %d/nil, want error", added)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key Bloom filter left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if added, err := ht.AddBloomFilterChecked("string", func() {}); err == nil {
		t.Fatalf("AddBloomFilterChecked(replacement unsupported) = %d/nil, want error", added)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}
	if hit, err := ht.HasBloomFilterChecked("seen", func() {}); err == nil {
		t.Fatalf("HasBloomFilterChecked(unsupported) = %v/nil, want error", hit)
	}
	if got := ht.AddBloomFilter("legacy", func() {}); got != 0 {
		t.Fatalf("AddBloomFilter legacy unsupported = %d, want 0", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected Bloom filter left value %+v", got)
	}
	if ht.HasBloomFilter("seen", func() {}) {
		t.Fatal("HasBloomFilter legacy unsupported = true, want false")
	}
}

func TestBloomFilterRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	tests := []struct {
		name              string
		expectedItems     uint64
		falsePositiveRate float64
	}{
		{name: "zero expected items", expectedItems: 0, falsePositiveRate: 0.01},
		{name: "zero false positive rate", expectedItems: 100, falsePositiveRate: 0},
		{name: "one false positive rate", expectedItems: 100, falsePositiveRate: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ht.UpsertBloomFilter("bad", tt.expectedItems, tt.falsePositiveRate); err == nil {
				t.Fatal("UpsertBloomFilter() error = nil, want error")
			}
			if got := ht.Get("bad"); !got.Empty() {
				t.Fatalf("invalid Bloom filter config stored value %+v", got)
			}
		})
	}
}

func TestBloomFilterStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertBloomFilter("seen", 100, 0.01); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	idx := ht.Get("seen").Index
	ht.UpsertString("seen", "value")
	if !bloomFilterIndexReleased(ht, idx) {
		t.Fatalf("overwritten Bloom filter index %d was not released", idx)
	}

	if err := ht.UpsertBloomFilter("new", 100, 0.01); err != nil {
		t.Fatalf("UpsertBloomFilter(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Bloom filter storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestCuckooFilterOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertCuckooFilter("seen", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	if hval := ht.Get("seen"); !hval.IsCuckooFilter() {
		t.Fatalf("UpsertCuckooFilter stored type %+v, want cuckoo filter", hval)
	}
	if added := ht.AddCuckooFilter("seen", "alpha", "beta", "alpha"); added != 2 {
		t.Fatalf("AddCuckooFilter() = %d, want 2 new values", added)
	}
	if !ht.HasCuckooFilter("seen", "alpha") || !ht.HasCuckooFilter("seen", "beta") {
		t.Fatal("HasCuckooFilter(inserted values) = false, want true")
	}
	if missing := cuckooFilterMissingValue(t, ht, "seen"); ht.HasCuckooFilter("seen", missing) {
		t.Fatal("HasCuckooFilter(missing) = true, want false")
	}
	if deleted := ht.DeleteCuckooFilter("seen", "alpha", "missing"); deleted != 1 {
		t.Fatalf("DeleteCuckooFilter(alpha, missing) = %d, want 1", deleted)
	}
	if ht.HasCuckooFilter("seen", "alpha") {
		t.Fatal("HasCuckooFilter(alpha after delete) = true, want false")
	}
	info, ok := ht.CuckooFilterInfo("seen")
	if !ok {
		t.Fatal("CuckooFilterInfo(seen) = false, want true")
	}
	if info.Count != 1 || info.BucketSize != cuckooFilterBucketSize || info.FingerprintBits == 0 || info.FingerprintBytes == 0 || info.Capacity < 128 {
		t.Fatalf("CuckooFilterInfo(seen) = %#v, want populated compact filter", info)
	}

	if err := ht.UpsertCuckooFilter("seen", 16, 0.1); err != nil {
		t.Fatalf("UpsertCuckooFilter(replace) error = %v", err)
	}
	if ht.HasCuckooFilter("seen", "beta") {
		t.Fatal("replacement Cuckoo filter retained old value")
	}
	if added := ht.AddCuckooFilter("auto", "value"); added != 1 {
		t.Fatalf("AddCuckooFilter(auto) = %d, want 1", added)
	}
	if hval := ht.Get("auto"); !hval.IsCuckooFilter() {
		t.Fatalf("AddCuckooFilter(auto) stored type %+v, want cuckoo filter", hval)
	}
}

func TestCuckooFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if added, err := ht.AddCuckooFilterChecked("seen", "alpha"); err != nil || added != 1 {
		t.Fatalf("AddCuckooFilterChecked(alpha) = %d/%v, want 1/nil", added, err)
	}

	if added, err := ht.AddCuckooFilterChecked("seen", "beta", func() {}); err == nil {
		t.Fatalf("AddCuckooFilterChecked(unsupported batch) = %d/nil, want error", added)
	}
	info, ok := ht.CuckooFilterInfo("seen")
	if !ok || info.Count != 1 {
		t.Fatalf("CuckooFilterInfo(after rejected add) = %#v/%v, want one item", info, ok)
	}
	if !ht.HasCuckooFilter("seen", "alpha") {
		t.Fatal("rejected add removed existing Cuckoo filter value")
	}
	if deleted, err := ht.DeleteCuckooFilterChecked("seen", "alpha", func() {}); err == nil {
		t.Fatalf("DeleteCuckooFilterChecked(unsupported batch) = %d/nil, want error", deleted)
	}
	info, ok = ht.CuckooFilterInfo("seen")
	if !ok || info.Count != 1 {
		t.Fatalf("CuckooFilterInfo(after rejected delete) = %#v/%v, want one item", info, ok)
	}
	if !ht.HasCuckooFilter("seen", "alpha") {
		t.Fatal("rejected delete removed existing Cuckoo filter value")
	}

	if added, err := ht.AddCuckooFilterChecked("missing", func() {}); err == nil {
		t.Fatalf("AddCuckooFilterChecked(missing unsupported) = %d/nil, want error", added)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key Cuckoo filter left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if added, err := ht.AddCuckooFilterChecked("string", func() {}); err == nil {
		t.Fatalf("AddCuckooFilterChecked(replacement unsupported) = %d/nil, want error", added)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}
	if hit, err := ht.HasCuckooFilterChecked("seen", func() {}); err == nil {
		t.Fatalf("HasCuckooFilterChecked(unsupported) = %v/nil, want error", hit)
	}
	if deleted, err := ht.DeleteCuckooFilterChecked("seen", func() {}); err == nil {
		t.Fatalf("DeleteCuckooFilterChecked(unsupported) = %d/nil, want error", deleted)
	}
	if got := ht.AddCuckooFilter("legacy", func() {}); got != 0 {
		t.Fatalf("AddCuckooFilter legacy unsupported = %d, want 0", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected Cuckoo filter left value %+v", got)
	}
	if ht.HasCuckooFilter("seen", func() {}) {
		t.Fatal("HasCuckooFilter legacy unsupported = true, want false")
	}
	if got := ht.DeleteCuckooFilter("seen", func() {}); got != 0 {
		t.Fatalf("DeleteCuckooFilter legacy unsupported = %d, want 0", got)
	}
}

func TestCuckooFilterRelocatesIntoReachableEmptyBucket(t *testing.T) {
	filter := newCuckooFilterDataWithShape(4, minCuckooFilterFingerprintBits)
	mask := cuckooFilterFingerprintMask(filter.fingerprintBits)

	for candidate := 0; candidate < 10000; candidate++ {
		value := "relocate-" + strconv.Itoa(candidate)
		hash, fp, index, alternate := cuckooFilterPlacement(t, &filter, value)
		currentIndex := index
		if (splitmix64(hash)^uint64(fp))&1 == 1 {
			currentIndex = alternate
		}
		relocationSlot := int(splitmix64(hash+uint64(fp)) % uint64(cuckooFilterBucketSize))

		for evicted := uint16(1); evicted <= mask; evicted++ {
			if evicted == fp {
				continue
			}
			target := filter.alternateIndex(currentIndex, evicted)
			if target == index || target == alternate {
				continue
			}

			filter = newCuckooFilterDataWithShape(4, minCuckooFilterFingerprintBits)
			fillCuckooBucketExcluding(t, &filter, index, fp)
			fillCuckooBucketExcluding(t, &filter, alternate, fp)
			filter.fingerprints[filter.bucketOffset(currentIndex, relocationSlot)] = evicted
			filter.count = 2 * uint64(cuckooFilterBucketSize)

			if !cuckooBucketFull(&filter, index) || !cuckooBucketFull(&filter, alternate) || filter.containsFingerprint(index, alternate, fp) {
				continue
			}

			if !filter.Add(value) {
				t.Fatalf("Add(%q) = false, want relocation into bucket %d", value, target)
			}
			if filter.count != 2*uint64(cuckooFilterBucketSize)+1 {
				t.Fatalf("count = %d, want %d after relocation", filter.count, 2*uint64(cuckooFilterBucketSize)+1)
			}
			if !filter.Contains(value) {
				t.Fatalf("Contains(%q) = false after relocation", value)
			}
			if !filter.bucketContains(target, evicted) {
				t.Fatalf("target bucket %d does not contain relocated fingerprint %d", target, evicted)
			}
			return
		}
	}
	t.Fatal("could not build a deterministic Cuckoo filter relocation scenario")
}

func TestCuckooFilterRelocationFailureRollsBack(t *testing.T) {
	filter := newCuckooFilterDataWithShape(2, minCuckooFilterFingerprintBits)
	mask := cuckooFilterFingerprintMask(filter.fingerprintBits)
	for idx := range filter.fingerprints {
		filter.fingerprints[idx] = uint16(idx%int(mask)) + 1
	}
	filter.count = uint64(len(filter.fingerprints))
	before := append([]uint16(nil), filter.fingerprints...)

	for candidate := 0; candidate < 10000; candidate++ {
		value := "full-" + strconv.Itoa(candidate)
		_, fp, index, alternate := cuckooFilterPlacement(t, &filter, value)
		if filter.containsFingerprint(index, alternate, fp) {
			continue
		}

		if filter.Add(value) {
			t.Fatalf("Add(%q) = true on full Cuckoo filter, want failure", value)
		}
		if filter.count != uint64(len(before)) {
			t.Fatalf("count = %d after failed relocation, want %d", filter.count, len(before))
		}
		if !reflect.DeepEqual(filter.fingerprints, before) {
			t.Fatalf("fingerprints changed after failed relocation: got %#v, want %#v", filter.fingerprints, before)
		}
		return
	}
	t.Fatal("could not find a non-matching value for a full Cuckoo filter")
}

func cuckooFilterPlacement(t *testing.T, filter *cuckooFilterData, value interface{}) (uint64, uint16, uint64, uint64) {
	t.Helper()
	key, err := cuckooFilterItemKey(value)
	if err != nil {
		t.Fatalf("cuckooFilterItemKey(%#v) error = %v", value, err)
	}
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	return hash, fp, index, filter.alternateIndex(index, fp)
}

func fillCuckooBucketExcluding(t *testing.T, filter *cuckooFilterData, index uint64, excluded ...uint16) {
	t.Helper()
	next := uint16(1)
	for slot := 0; slot < int(cuckooFilterBucketSize); slot++ {
		for cuckooFingerprintExcluded(next, excluded) {
			next++
			if next > cuckooFilterFingerprintMask(filter.fingerprintBits) {
				t.Fatalf("not enough Cuckoo fingerprints to fill bucket %d", index)
			}
		}
		filter.fingerprints[filter.bucketOffset(index, slot)] = next
		next++
	}
}

func cuckooFingerprintExcluded(fp uint16, excluded []uint16) bool {
	for _, candidate := range excluded {
		if fp == candidate {
			return true
		}
	}
	return false
}

func cuckooBucketFull(filter *cuckooFilterData, index uint64) bool {
	for slot := 0; slot < int(cuckooFilterBucketSize); slot++ {
		if filter.fingerprints[filter.bucketOffset(index, slot)] == 0 {
			return false
		}
	}
	return true
}

func TestCuckooFilterRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	tests := []struct {
		name              string
		capacity          uint64
		falsePositiveRate float64
	}{
		{name: "zero capacity", capacity: 0, falsePositiveRate: 0.01},
		{name: "zero fpr", capacity: 100, falsePositiveRate: 0},
		{name: "one fpr", capacity: 100, falsePositiveRate: 1},
		{name: "too small fpr", capacity: 100, falsePositiveRate: math.SmallestNonzeroFloat64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ht.UpsertCuckooFilter("bad", tt.capacity, tt.falsePositiveRate); err == nil {
				t.Fatal("UpsertCuckooFilter() error = nil, want error")
			}
			if got := ht.Get("bad"); !got.Empty() {
				t.Fatalf("invalid Cuckoo filter config stored value %+v", got)
			}
		})
	}
}

func TestCuckooFilterSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	filter, err := newCuckooFilterData(32, 0.01)
	if err != nil {
		t.Fatalf("newCuckooFilterData() error = %v", err)
	}
	filter.Add("alpha")
	snapshot := filter.Snapshot()
	snapshot.Count++
	if err := validateCuckooFilterSnapshot(snapshot); err == nil {
		t.Fatal("validateCuckooFilterSnapshot(mismatched count) error = nil, want error")
	}

	snapshot = filter.Snapshot()
	raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	binary.LittleEndian.PutUint16(raw[:2], cuckooFilterFingerprintMask(snapshot.FingerprintBits)+1)
	snapshot.Fingerprints = base64.StdEncoding.EncodeToString(raw)
	if err := validateCuckooFilterSnapshot(snapshot); err == nil {
		t.Fatal("validateCuckooFilterSnapshot(invalid fingerprint) error = nil, want error")
	}
}

func TestCuckooFilterStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertCuckooFilter("seen", 100, 0.01); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	idx := ht.Get("seen").Index
	ht.UpsertString("seen", "value")
	if !cuckooFilterIndexReleased(ht, idx) {
		t.Fatalf("overwritten Cuckoo filter index %d was not released", idx)
	}

	if err := ht.UpsertCuckooFilter("new", 100, 0.01); err != nil {
		t.Fatalf("UpsertCuckooFilter(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Cuckoo filter storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestRoaringBitmapOperations(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertRoaringBitmap("ids")
	if hval := ht.Get("ids"); !hval.IsRoaringBitmap() {
		t.Fatalf("UpsertRoaringBitmap stored type %+v, want roaring bitmap", hval)
	}
	if added := ht.AddRoaringBitmap("ids", 1, 2, 1, 1<<16+7, ^uint32(0)); added != 4 {
		t.Fatalf("AddRoaringBitmap() = %d, want 4 unique values", added)
	}
	if !ht.HasRoaringBitmap("ids", 1) || !ht.HasRoaringBitmap("ids", 1<<16+7) || !ht.HasRoaringBitmap("ids", ^uint32(0)) {
		t.Fatal("HasRoaringBitmap(inserted values) = false, want true")
	}
	if ht.HasRoaringBitmap("ids", 3) {
		t.Fatal("HasRoaringBitmap(missing) = true, want false")
	}
	if removed := ht.RemoveRoaringBitmap("ids", 2, 3); removed != 1 {
		t.Fatalf("RemoveRoaringBitmap(2, 3) = %d, want 1", removed)
	}
	if count, ok := ht.CountRoaringBitmap("ids"); !ok || count != 3 {
		t.Fatalf("CountRoaringBitmap(ids) = %d/%v, want 3", count, ok)
	}
	if got := ht.GetRoaringBitmap("ids"); !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7, ^uint32(0)}) {
		t.Fatalf("GetRoaringBitmap(ids) = %#v, want sorted values", got)
	}
	info, ok := ht.RoaringBitmapInfo("ids")
	if !ok {
		t.Fatal("RoaringBitmapInfo(ids) = false, want true")
	}
	if info.Cardinality != 3 || info.Containers != 3 || info.ArrayContainers != 3 || info.EncodedBytes != 6 {
		t.Fatalf("RoaringBitmapInfo(ids) = %#v, want sparse compact arrays", info)
	}
	idx := ht.Get("ids").Index
	ht.UpsertRoaringBitmap("ids")
	if got := ht.Get("ids"); !got.IsRoaringBitmap() || got.Index != idx {
		t.Fatalf("UpsertRoaringBitmap replacement stored %+v, want same roaring bitmap slot %d", got, idx)
	}
	if count, ok := ht.CountRoaringBitmap("ids"); !ok || count != 0 {
		t.Fatalf("CountRoaringBitmap(ids after replacement) = %d/%v, want empty bitmap", count, ok)
	}

	if added := ht.AddRoaringBitmap("auto", 42); added != 1 {
		t.Fatalf("AddRoaringBitmap(auto) = %d, want 1", added)
	}
	if hval := ht.Get("auto"); !hval.IsRoaringBitmap() {
		t.Fatalf("AddRoaringBitmap(auto) stored type %+v, want roaring bitmap", hval)
	}
}

func TestCheckedRoaringBitmapOperations(t *testing.T) {
	ht := newTestTrie(t)

	added, err := ht.AddRoaringBitmapChecked("ids", 3, 1, 3, 1<<16+7)
	if err != nil || added != 3 {
		t.Fatalf("AddRoaringBitmapChecked(ids) = %d/%v, want 3/nil", added, err)
	}
	if hit, err := ht.HasRoaringBitmapChecked("ids", 1<<16+7); err != nil || !hit {
		t.Fatalf("HasRoaringBitmapChecked(inserted) = %v/%v, want true/nil", hit, err)
	}
	if hit, err := ht.HasRoaringBitmapChecked("ids", 2); err != nil || hit {
		t.Fatalf("HasRoaringBitmapChecked(missing) = %v/%v, want false/nil", hit, err)
	}
	if removed, err := ht.RemoveRoaringBitmapChecked("ids", 3, 4); err != nil || removed != 1 {
		t.Fatalf("RemoveRoaringBitmapChecked(ids) = %d/%v, want 1/nil", removed, err)
	}
	if count, ok, err := ht.CountRoaringBitmapChecked("ids"); err != nil || !ok || count != 2 {
		t.Fatalf("CountRoaringBitmapChecked(ids) = %d/%v/%v, want 2/true/nil", count, ok, err)
	}
	if got, ok, err := ht.GetRoaringBitmapChecked("ids"); err != nil || !ok || !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7}) {
		t.Fatalf("GetRoaringBitmapChecked(ids) = %#v/%v/%v, want sorted values", got, ok, err)
	}
	if info, ok, err := ht.RoaringBitmapInfoChecked("ids"); err != nil || !ok || info.Cardinality != 2 || info.Containers != 2 {
		t.Fatalf("RoaringBitmapInfoChecked(ids) = %#v/%v/%v, want two values", info, ok, err)
	}

	ht.UpsertString("string", "value")
	if _, err := ht.AddRoaringBitmapChecked("string", 9); err != nil {
		t.Fatalf("AddRoaringBitmapChecked(replace string) error = %v", err)
	}
	if hval := ht.Get("string"); !hval.IsRoaringBitmap() {
		t.Fatalf("AddRoaringBitmapChecked(replace string) stored %+v, want roaring bitmap", hval)
	}
	if removed, err := ht.RemoveRoaringBitmapChecked("missing", 1); err != nil || removed != 0 {
		t.Fatalf("RemoveRoaringBitmapChecked(missing) = %d/%v, want 0/nil", removed, err)
	}
	if _, ok, err := ht.GetRoaringBitmapChecked("missing"); err != nil || ok {
		t.Fatalf("GetRoaringBitmapChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}
}

func TestRoaringBitmapConvertsDenseContainers(t *testing.T) {
	ht := newTestTrie(t)
	for idx := 0; idx <= roaringBitmapArrayMaxSize; idx++ {
		ht.AddRoaringBitmap("dense", uint32(idx))
	}
	info, ok := ht.RoaringBitmapInfo("dense")
	if !ok {
		t.Fatal("RoaringBitmapInfo(dense) = false, want true")
	}
	if info.BitmapContainers != 1 || info.ArrayContainers != 0 || info.EncodedBytes != roaringBitmapBitmapWords*8 {
		t.Fatalf("dense RoaringBitmapInfo = %#v, want one bitmap container", info)
	}
	for idx := roaringBitmapArrayShrinkSize; idx <= roaringBitmapArrayMaxSize; idx++ {
		ht.RemoveRoaringBitmap("dense", uint32(idx))
	}
	info, ok = ht.RoaringBitmapInfo("dense")
	if !ok {
		t.Fatal("RoaringBitmapInfo(dense after shrink) = false, want true")
	}
	if info.ArrayContainers != 1 || info.BitmapContainers != 0 || info.EncodedBytes != uint64(roaringBitmapArrayShrinkSize*2) {
		t.Fatalf("shrunk RoaringBitmapInfo = %#v, want one compact array container", info)
	}
}

func TestRoaringBitmapRejectsInvalidCommandValues(t *testing.T) {
	if _, err := roaringBitmapValuesFromCommand(CacheCommandRequest{Values: Slice{"1", json.Number("4294967295")}}); err != nil {
		t.Fatalf("roaringBitmapValuesFromCommand(valid) error = %v", err)
	}
	tests := []CacheCommandRequest{
		{},
		{Value: "-1"},
		{Value: "4294967296"},
		{Values: Slice{1.5}},
	}
	for _, request := range tests {
		if _, err := roaringBitmapValuesFromCommand(request); err == nil {
			t.Fatalf("roaringBitmapValuesFromCommand(%#v) error = nil, want error", request)
		}
	}
}

func TestRoaringBitmapSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	data := newRoaringBitmapData()
	for idx := 0; idx <= roaringBitmapArrayMaxSize; idx++ {
		data.Add(uint32(idx))
	}
	snapshot := data.Snapshot()
	snapshot.Cardinality++
	if err := validateRoaringBitmapSnapshot(snapshot); err == nil {
		t.Fatal("validateRoaringBitmapSnapshot(mismatched cardinality) error = nil, want error")
	}

	snapshot = data.Snapshot()
	snapshot.Containers[0].Cardinality++
	if err := validateRoaringBitmapSnapshot(snapshot); err == nil {
		t.Fatal("validateRoaringBitmapSnapshot(mismatched container cardinality) error = nil, want error")
	}

	snapshot = data.Snapshot()
	snapshot.Containers[0].Kind = "unknown"
	if err := validateRoaringBitmapSnapshot(snapshot); err == nil {
		t.Fatal("validateRoaringBitmapSnapshot(unknown kind) error = nil, want error")
	}
}

func TestRoaringBitmapStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertRoaringBitmap("ids")
	idx := ht.Get("ids").Index
	ht.UpsertString("ids", "value")
	if !roaringBitmapIndexReleased(ht, idx) {
		t.Fatalf("overwritten Roaring bitmap index %d was not released", idx)
	}

	ht.UpsertRoaringBitmap("new")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Roaring bitmap storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestSparseBitsetOperations(t *testing.T) {
	ht := newTestTrie(t)
	maxID := ^uint64(0)

	ht.UpsertSparseBitset("ids")
	if hval := ht.Get("ids"); !hval.IsSparseBitset() {
		t.Fatalf("UpsertSparseBitset stored type %+v, want sparse bitset", hval)
	}
	if added := ht.AddSparseBitset("ids", 1, 2, 1, 1<<32+7, maxID); added != 4 {
		t.Fatalf("AddSparseBitset() = %d, want 4 unique values", added)
	}
	if !ht.HasSparseBitset("ids", 1) || !ht.HasSparseBitset("ids", 1<<32+7) || !ht.HasSparseBitset("ids", maxID) {
		t.Fatal("HasSparseBitset(inserted values) = false, want true")
	}
	if ht.HasSparseBitset("ids", 3) {
		t.Fatal("HasSparseBitset(missing) = true, want false")
	}
	if removed := ht.RemoveSparseBitset("ids", 2, 3); removed != 1 {
		t.Fatalf("RemoveSparseBitset(2, 3) = %d, want 1", removed)
	}
	if count, ok := ht.CountSparseBitset("ids"); !ok || count != 3 {
		t.Fatalf("CountSparseBitset(ids) = %d/%v, want 3", count, ok)
	}
	if got := ht.GetSparseBitset("ids"); !reflect.DeepEqual(got, []uint64{1, 1<<32 + 7, maxID}) {
		t.Fatalf("GetSparseBitset(ids) = %#v, want sorted uint64 values", got)
	}
	info, ok := ht.SparseBitsetInfo("ids")
	if !ok {
		t.Fatal("SparseBitsetInfo(ids) = false, want true")
	}
	if info.Cardinality != 3 || info.Containers != 3 || info.ArrayContainers != 3 || info.EncodedBytes != 6 {
		t.Fatalf("SparseBitsetInfo(ids) = %#v, want sparse compact arrays", info)
	}
	idx := ht.Get("ids").Index
	ht.UpsertSparseBitset("ids")
	if got := ht.Get("ids"); !got.IsSparseBitset() || got.Index != idx {
		t.Fatalf("UpsertSparseBitset replacement stored %+v, want same sparse bitset slot %d", got, idx)
	}
	if count, ok := ht.CountSparseBitset("ids"); !ok || count != 0 {
		t.Fatalf("CountSparseBitset(ids after replacement) = %d/%v, want empty bitset", count, ok)
	}

	if added := ht.AddSparseBitset("auto", 42); added != 1 {
		t.Fatalf("AddSparseBitset(auto) = %d, want 1", added)
	}
	if hval := ht.Get("auto"); !hval.IsSparseBitset() {
		t.Fatalf("AddSparseBitset(auto) stored type %+v, want sparse bitset", hval)
	}
}

func TestCheckedSparseBitsetOperations(t *testing.T) {
	ht := newTestTrie(t)
	maxID := ^uint64(0)

	added, err := ht.AddSparseBitsetChecked("ids", 3, 1, 3, 1<<32+7, maxID)
	if err != nil || added != 4 {
		t.Fatalf("AddSparseBitsetChecked(ids) = %d/%v, want 4/nil", added, err)
	}
	if hit, err := ht.HasSparseBitsetChecked("ids", maxID); err != nil || !hit {
		t.Fatalf("HasSparseBitsetChecked(inserted) = %v/%v, want true/nil", hit, err)
	}
	if hit, err := ht.HasSparseBitsetChecked("ids", 2); err != nil || hit {
		t.Fatalf("HasSparseBitsetChecked(missing) = %v/%v, want false/nil", hit, err)
	}
	if removed, err := ht.RemoveSparseBitsetChecked("ids", 3, 4); err != nil || removed != 1 {
		t.Fatalf("RemoveSparseBitsetChecked(ids) = %d/%v, want 1/nil", removed, err)
	}
	if count, ok, err := ht.CountSparseBitsetChecked("ids"); err != nil || !ok || count != 3 {
		t.Fatalf("CountSparseBitsetChecked(ids) = %d/%v/%v, want 3/true/nil", count, ok, err)
	}
	if got, ok, err := ht.GetSparseBitsetChecked("ids"); err != nil || !ok || !reflect.DeepEqual(got, []uint64{1, 1<<32 + 7, maxID}) {
		t.Fatalf("GetSparseBitsetChecked(ids) = %#v/%v/%v, want sorted values", got, ok, err)
	}
	if info, ok, err := ht.SparseBitsetInfoChecked("ids"); err != nil || !ok || info.Cardinality != 3 || info.Containers != 3 {
		t.Fatalf("SparseBitsetInfoChecked(ids) = %#v/%v/%v, want three values", info, ok, err)
	}

	ht.UpsertString("string", "value")
	if _, err := ht.AddSparseBitsetChecked("string", 9); err != nil {
		t.Fatalf("AddSparseBitsetChecked(replace string) error = %v", err)
	}
	if hval := ht.Get("string"); !hval.IsSparseBitset() {
		t.Fatalf("AddSparseBitsetChecked(replace string) stored %+v, want sparse bitset", hval)
	}
	if removed, err := ht.RemoveSparseBitsetChecked("missing", 1); err != nil || removed != 0 {
		t.Fatalf("RemoveSparseBitsetChecked(missing) = %d/%v, want 0/nil", removed, err)
	}
	if _, ok, err := ht.GetSparseBitsetChecked("missing"); err != nil || ok {
		t.Fatalf("GetSparseBitsetChecked(missing) ok/error = %v/%v, want false/nil", ok, err)
	}
}

func TestSparseBitsetConvertsDenseContainers(t *testing.T) {
	ht := newTestTrie(t)
	for idx := 0; idx <= sparseBitsetArrayMaxSize; idx++ {
		ht.AddSparseBitset("dense", uint64(idx))
	}
	info, ok := ht.SparseBitsetInfo("dense")
	if !ok {
		t.Fatal("SparseBitsetInfo(dense) = false, want true")
	}
	if info.BitmapContainers != 1 || info.ArrayContainers != 0 || info.EncodedBytes != sparseBitsetBitmapWords*8 {
		t.Fatalf("dense SparseBitsetInfo = %#v, want one bitmap container", info)
	}
	for idx := sparseBitsetArrayShrinkSize; idx <= sparseBitsetArrayMaxSize; idx++ {
		ht.RemoveSparseBitset("dense", uint64(idx))
	}
	info, ok = ht.SparseBitsetInfo("dense")
	if !ok {
		t.Fatal("SparseBitsetInfo(dense after shrink) = false, want true")
	}
	if info.ArrayContainers != 1 || info.BitmapContainers != 0 || info.EncodedBytes != uint64(sparseBitsetArrayShrinkSize*2) {
		t.Fatalf("shrunk SparseBitsetInfo = %#v, want one compact array container", info)
	}
}

func TestSparseBitsetRejectsInvalidCommandValues(t *testing.T) {
	if _, err := sparseBitsetValuesFromCommand(CacheCommandRequest{Values: Slice{"1", json.Number("18446744073709551615")}}); err != nil {
		t.Fatalf("sparseBitsetValuesFromCommand(valid) error = %v", err)
	}
	tests := []CacheCommandRequest{
		{},
		{Value: "-1"},
		{Value: "18446744073709551616"},
		{Values: Slice{1.5}},
	}
	for _, request := range tests {
		if _, err := sparseBitsetValuesFromCommand(request); err == nil {
			t.Fatalf("sparseBitsetValuesFromCommand(%#v) error = nil, want error", request)
		}
	}
}

func TestSparseBitsetSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	data := newSparseBitsetData()
	for idx := 0; idx <= sparseBitsetArrayMaxSize; idx++ {
		data.Add(uint64(idx))
	}
	snapshot := data.Snapshot()
	snapshot.Cardinality++
	if err := validateSparseBitsetSnapshot(snapshot); err == nil {
		t.Fatal("validateSparseBitsetSnapshot(mismatched cardinality) error = nil, want error")
	}

	snapshot = data.Snapshot()
	snapshot.Containers[0].Cardinality++
	if err := validateSparseBitsetSnapshot(snapshot); err == nil {
		t.Fatal("validateSparseBitsetSnapshot(mismatched container cardinality) error = nil, want error")
	}

	snapshot = data.Snapshot()
	snapshot.Containers[0].Kind = "unknown"
	if err := validateSparseBitsetSnapshot(snapshot); err == nil {
		t.Fatal("validateSparseBitsetSnapshot(unknown kind) error = nil, want error")
	}
}

func TestSparseBitsetStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertSparseBitset("ids")
	idx := ht.Get("ids").Index
	ht.UpsertString("ids", "value")
	if !sparseBitsetIndexReleased(ht, idx) {
		t.Fatalf("overwritten sparse bitset index %d was not released", idx)
	}

	ht.UpsertSparseBitset("new")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("sparse bitset storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestCountMinSketchOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertCountMinSketch("freq", 128, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	if hval := ht.Get("freq"); !hval.IsCountMinSketch() {
		t.Fatalf("UpsertCountMinSketch stored type %+v, want count-min sketch", hval)
	}
	if got := ht.IncrementCountMinSketch("freq", "alpha", 2); got < 2 {
		t.Fatalf("IncrementCountMinSketch(alpha, 2) = %d, want at least 2", got)
	}
	if got := ht.IncrementCountMinSketch("freq", "alpha", 3); got < 5 {
		t.Fatalf("IncrementCountMinSketch(alpha, 3) = %d, want at least 5", got)
	}
	if got := ht.IncrementCountMinSketch("freq", "beta", 1); got < 1 {
		t.Fatalf("IncrementCountMinSketch(beta, 1) = %d, want at least 1", got)
	}
	if got, ok := ht.EstimateCountMinSketch("freq", "alpha"); !ok || got < 5 {
		t.Fatalf("EstimateCountMinSketch(alpha) = %d/%v, want at least 5", got, ok)
	}
	info, ok := ht.CountMinSketchInfo("freq")
	if !ok {
		t.Fatal("CountMinSketchInfo(freq) = false, want true")
	}
	if info.Width != 128 || info.Depth != 4 || info.CounterBytes != 128*4*4 || info.TotalCount != 6 || info.MaxCounter < 5 {
		t.Fatalf("CountMinSketchInfo(freq) = %#v, want populated compact sketch", info)
	}

	if err := ht.UpsertCountMinSketch("freq", 32, 3); err != nil {
		t.Fatalf("UpsertCountMinSketch(replace) error = %v", err)
	}
	if got, ok := ht.EstimateCountMinSketch("freq", "alpha"); !ok || got != 0 {
		t.Fatalf("EstimateCountMinSketch(alpha after replace) = %d/%v, want 0 hit", got, ok)
	}
	if got := ht.IncrementCountMinSketch("auto", "value", 1); got < 1 {
		t.Fatalf("IncrementCountMinSketch(auto) = %d, want at least 1", got)
	}
	if hval := ht.Get("auto"); !hval.IsCountMinSketch() {
		t.Fatalf("IncrementCountMinSketch(auto) stored type %+v, want count-min sketch", hval)
	}
	if got := ht.IncrementCountMinSketch("noop", "value", 0); got != 0 {
		t.Fatalf("IncrementCountMinSketch(noop, 0) = %d, want 0", got)
	}
	if hval := ht.Get("noop"); !hval.Empty() {
		t.Fatalf("zero-count Count-Min Sketch increment created value %+v", hval)
	}
}

func TestCountMinSketchRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if estimate, err := ht.IncrementCountMinSketchChecked("freq", "alpha", 2); err != nil || estimate < 2 {
		t.Fatalf("IncrementCountMinSketchChecked(alpha) = %d/%v, want estimate at least 2", estimate, err)
	}

	if estimate, err := ht.IncrementCountMinSketchChecked("freq", "beta", 1, func() {}); err == nil {
		t.Fatalf("IncrementCountMinSketchChecked(unsupported batch) = %d/nil, want error", estimate)
	}
	info, ok := ht.CountMinSketchInfo("freq")
	if !ok || info.TotalCount != 2 {
		t.Fatalf("CountMinSketchInfo(after rejected batch) = %#v/%v, want total count 2", info, ok)
	}
	if estimate, ok := ht.EstimateCountMinSketch("freq", "alpha"); !ok || estimate < 2 {
		t.Fatalf("EstimateCountMinSketch(alpha after rejected batch) = %d/%v, want retained alpha", estimate, ok)
	}

	if estimate, err := ht.IncrementCountMinSketchChecked("missing", func() {}, 1); err == nil {
		t.Fatalf("IncrementCountMinSketchChecked(missing unsupported) = %d/nil, want error", estimate)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key Count-Min Sketch left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if estimate, err := ht.IncrementCountMinSketchChecked("string", func() {}, 1); err == nil {
		t.Fatalf("IncrementCountMinSketchChecked(replacement unsupported) = %d/nil, want error", estimate)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}
	if estimate, ok, err := ht.EstimateCountMinSketchChecked("freq", func() {}); err == nil {
		t.Fatalf("EstimateCountMinSketchChecked(unsupported) = %d/%v/nil, want error", estimate, ok)
	}
	if got := ht.IncrementCountMinSketch("legacy", func() {}, 1); got != 0 {
		t.Fatalf("IncrementCountMinSketch legacy unsupported = %d, want 0", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected Count-Min Sketch left value %+v", got)
	}
	if estimate, ok := ht.EstimateCountMinSketch("freq", func() {}); estimate != 0 || ok {
		t.Fatalf("EstimateCountMinSketch legacy unsupported = %d/%v, want 0/false", estimate, ok)
	}
}

func TestCountMinSketchRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	tests := []struct {
		name  string
		width uint64
		depth uint8
	}{
		{name: "zero width", width: 0, depth: 4},
		{name: "zero depth", width: 128, depth: 0},
		{name: "too many counters", width: maxCountMinSketchCounters, depth: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ht.UpsertCountMinSketch("bad", tt.width, tt.depth); err == nil {
				t.Fatal("UpsertCountMinSketch() error = nil, want error")
			}
			if got := ht.Get("bad"); !got.Empty() {
				t.Fatalf("invalid Count-Min Sketch config stored value %+v", got)
			}
		})
	}
}

func TestCountMinSketchCountersSaturate(t *testing.T) {
	sketch, err := newCountMinSketchData(16, 3)
	if err != nil {
		t.Fatalf("newCountMinSketchData() error = %v", err)
	}
	if got := sketch.Add("hot", maxCountMinSketchCounter); got != uint64(maxCountMinSketchCounter) {
		t.Fatalf("Add(max counter) = %d, want saturation value", got)
	}
	if got := sketch.Add("hot", 1); got != uint64(maxCountMinSketchCounter) {
		t.Fatalf("Add(after saturation) = %d, want saturated value", got)
	}
	info := sketch.Info()
	if info.SaturatedCounters != uint64(sketch.depth) || info.MaxCounter != maxCountMinSketchCounter {
		t.Fatalf("saturated Count-Min Sketch info = %#v, want one saturated counter per row", info)
	}
}

func TestCountMinSketchStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertCountMinSketch("freq", 64, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	idx := ht.Get("freq").Index
	ht.UpsertString("freq", "value")
	if !countMinSketchIndexReleased(ht, idx) {
		t.Fatalf("overwritten Count-Min Sketch index %d was not released", idx)
	}

	if err := ht.UpsertCountMinSketch("new", 64, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Count-Min Sketch storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestHyperLogLogOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertHyperLogLog("card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	if hval := ht.Get("card"); !hval.IsHyperLogLog() {
		t.Fatalf("UpsertHyperLogLog stored type %+v, want hyperloglog", hval)
	}
	if got := ht.AddHyperLogLog("card", "alpha", "beta", "alpha"); got < 2 {
		t.Fatalf("AddHyperLogLog(alpha, beta, alpha) = %d, want at least 2", got)
	}
	if got, ok := ht.CountHyperLogLog("card"); !ok || got < 2 {
		t.Fatalf("CountHyperLogLog(card) = %d/%v, want at least 2", got, ok)
	}
	info, ok := ht.HyperLogLogInfo("card")
	if !ok {
		t.Fatal("HyperLogLogInfo(card) = false, want true")
	}
	if info.Precision != 10 || info.RegisterCount != 1<<10 || info.RegisterBytes != 1<<10 || info.Observations != 3 || info.NonZeroRegisters < 2 || info.Estimate < 2 {
		t.Fatalf("HyperLogLogInfo(card) = %#v, want populated compact registers", info)
	}

	if err := ht.UpsertHyperLogLog("card", 8); err != nil {
		t.Fatalf("UpsertHyperLogLog(replace) error = %v", err)
	}
	if got, ok := ht.CountHyperLogLog("card"); !ok || got != 0 {
		t.Fatalf("CountHyperLogLog(card after replace) = %d/%v, want 0 hit", got, ok)
	}
	if got := ht.AddHyperLogLog("auto", "value"); got < 1 {
		t.Fatalf("AddHyperLogLog(auto) = %d, want at least 1", got)
	}
	if hval := ht.Get("auto"); !hval.IsHyperLogLog() {
		t.Fatalf("AddHyperLogLog(auto) stored type %+v, want hyperloglog", hval)
	}
}

func TestHyperLogLogRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if estimate, err := ht.AddHyperLogLogChecked("card", "alpha"); err != nil || estimate < 1 {
		t.Fatalf("AddHyperLogLogChecked(alpha) = %d/%v, want estimate at least 1", estimate, err)
	}

	if estimate, err := ht.AddHyperLogLogChecked("card", "beta", func() {}); err == nil {
		t.Fatalf("AddHyperLogLogChecked(unsupported batch) = %d/nil, want error", estimate)
	}
	info, ok := ht.HyperLogLogInfo("card")
	if !ok || info.Observations != 1 || info.Estimate < 1 {
		t.Fatalf("HyperLogLogInfo(after rejected batch) = %#v/%v, want one observation", info, ok)
	}

	if estimate, err := ht.AddHyperLogLogChecked("missing", func() {}); err == nil {
		t.Fatalf("AddHyperLogLogChecked(missing unsupported) = %d/nil, want error", estimate)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key HyperLogLog left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if estimate, err := ht.AddHyperLogLogChecked("string", func() {}); err == nil {
		t.Fatalf("AddHyperLogLogChecked(replacement unsupported) = %d/nil, want error", estimate)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}
	if got := ht.AddHyperLogLog("legacy", func() {}); got != 0 {
		t.Fatalf("AddHyperLogLog legacy unsupported = %d, want 0", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected HyperLogLog left value %+v", got)
	}
}

func TestHyperLogLogRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, precision := range []uint8{minHyperLogLogPrecision - 1, maxHyperLogLogPrecision + 1} {
		if err := ht.UpsertHyperLogLog("bad", precision); err == nil {
			t.Fatalf("UpsertHyperLogLog(%d) error = nil, want error", precision)
		}
		if got := ht.Get("bad"); !got.Empty() {
			t.Fatalf("invalid HyperLogLog config stored value %+v", got)
		}
	}
}

func TestHyperLogLogSnapshotValidationRejectsInvalidRegisterRank(t *testing.T) {
	hll, err := newHyperLogLogData(10)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	snapshot := hll.Snapshot()
	raw, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	raw[0] = hyperLogLogMaxRank(snapshot.Precision) + 1
	snapshot.Registers = base64.StdEncoding.EncodeToString(raw)
	if err := validateHyperLogLogSnapshot(snapshot); err == nil {
		t.Fatal("validateHyperLogLogSnapshot(invalid rank) error = nil, want error")
	}
}

func TestHyperLogLogStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertHyperLogLog("card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	idx := ht.Get("card").Index
	ht.UpsertString("card", "value")
	if !hyperLogLogIndexReleased(ht, idx) {
		t.Fatalf("overwritten HyperLogLog index %d was not released", idx)
	}

	if err := ht.UpsertHyperLogLog("new", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("HyperLogLog storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestTopKOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertTopK("top", 3); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	if hval := ht.Get("top"); !hval.IsTopK() {
		t.Fatalf("UpsertTopK stored type %+v, want top-k", hval)
	}
	if got := ht.AddTopK("top", "alpha", 5); !got.Tracked || got.Count != 5 {
		t.Fatalf("AddTopK(alpha, 5) = %#v, want tracked count 5", got)
	}
	ht.AddTopK("top", "beta", 3)
	ht.AddTopK("top", "gamma", 1)
	ht.AddTopK("top", "delta", 2)
	if got := ht.EstimateTopK("top", "alpha"); !got.Tracked || got.Count != 5 || got.Error != 0 {
		t.Fatalf("EstimateTopK(alpha) = %#v, want exact tracked count 5", got)
	}
	if got := ht.EstimateTopK("top", "gamma"); got.Tracked {
		t.Fatalf("EstimateTopK(evicted gamma) = %#v, want untracked", got)
	}
	items := ht.GetTopK("top")
	if len(items) != 3 {
		t.Fatalf("GetTopK len = %d, want capacity 3", len(items))
	}
	if items[0].Value != "alpha" || items[0].Count != 5 {
		t.Fatalf("top item = %#v, want alpha count 5", items[0])
	}
	info, ok := ht.TopKInfo("top")
	if !ok {
		t.Fatal("TopKInfo(top) = false, want true")
	}
	if info.Capacity != 3 || info.Tracked != 3 || info.Total != 11 || info.MaxCount != 5 || info.MinCount == 0 {
		t.Fatalf("TopKInfo(top) = %#v, want populated fixed-capacity sketch", info)
	}

	if err := ht.UpsertTopK("top", 2); err != nil {
		t.Fatalf("UpsertTopK(replace) error = %v", err)
	}
	if got := ht.GetTopK("top"); len(got) != 0 {
		t.Fatalf("GetTopK(after replace) = %#v, want empty sketch", got)
	}
	if got := ht.AddTopK("auto", "value", 1); !got.Tracked || got.Count != 1 {
		t.Fatalf("AddTopK(auto) = %#v, want tracked count 1", got)
	}
	if hval := ht.Get("auto"); !hval.IsTopK() {
		t.Fatalf("AddTopK(auto) stored type %+v, want top-k", hval)
	}
	if got := ht.AddTopK("noop", "value", 0); got.Tracked {
		t.Fatalf("AddTopK(noop, 0) = %#v, want untracked missing estimate", got)
	}
	if hval := ht.Get("noop"); !hval.Empty() {
		t.Fatalf("zero-count Top-K add created value %+v", hval)
	}
}

func TestTopKClonesNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	value := Map{"path": "/api/users"}
	if got := ht.AddTopK("top", value, 1); !got.Tracked {
		t.Fatalf("AddTopK() = %#v, want tracked", got)
	}
	value["path"] = "/caller"
	items := ht.GetTopK("top")
	if got := items[0].Value.(Map)["path"]; got != "/api/users" {
		t.Fatalf("stored Top-K value = %v, want cloned /api/users", got)
	}
	items[0].Value.(Map)["path"] = "/mutated"
	if got := ht.GetTopK("top")[0].Value.(Map)["path"]; got != "/api/users" {
		t.Fatalf("GetTopK exposed nested value: %v", got)
	}
}

func TestTopKRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if estimate, err := ht.AddTopKChecked("top", "alpha", 2); err != nil || !estimate.Tracked || estimate.Count != 2 {
		t.Fatalf("AddTopKChecked(alpha) = %#v/%v, want tracked count 2", estimate, err)
	}

	if estimate, err := ht.AddTopKChecked("top", "beta", 1, func() {}); err == nil {
		t.Fatalf("AddTopKChecked(unsupported batch) = %#v/nil, want error", estimate)
	}
	info, ok := ht.TopKInfo("top")
	if !ok || info.Total != 2 || info.Tracked != 1 {
		t.Fatalf("TopKInfo(after rejected batch) = %#v/%v, want unchanged one-item sketch", info, ok)
	}
	items := ht.GetTopK("top")
	if len(items) != 1 || items[0].Value != "alpha" || items[0].Count != 2 {
		t.Fatalf("GetTopK(after rejected batch) = %#v, want alpha count 2 only", items)
	}

	if estimate, err := ht.AddTopKChecked("missing", func() {}, 1); err == nil {
		t.Fatalf("AddTopKChecked(missing unsupported) = %#v/nil, want error", estimate)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key Top-K left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if estimate, err := ht.AddTopKChecked("string", func() {}, 1); err == nil {
		t.Fatalf("AddTopKChecked(replacement unsupported) = %#v/nil, want error", estimate)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}
	if estimate, err := ht.EstimateTopKChecked("top", func() {}); err == nil {
		t.Fatalf("EstimateTopKChecked(unsupported) = %#v/nil, want error", estimate)
	}
	if got := ht.AddTopK("legacy", func() {}, 1); got != (TopKEstimate{}) {
		t.Fatalf("AddTopK legacy unsupported = %#v, want zero estimate", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected Top-K left value %+v", got)
	}
	if got := ht.EstimateTopK("top", func() {}); got != (TopKEstimate{}) {
		t.Fatalf("EstimateTopK legacy unsupported = %#v, want zero estimate", got)
	}
}

func TestTopKRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, capacity := range []uint64{0, maxTopKCapacity + 1} {
		if err := ht.UpsertTopK("bad", capacity); err == nil {
			t.Fatalf("UpsertTopK(%d) error = nil, want error", capacity)
		}
		if got := ht.Get("bad"); !got.Empty() {
			t.Fatalf("invalid Top-K config stored value %+v", got)
		}
	}
}

func TestTopKSnapshotValidationRejectsDuplicateAndMismatchedKeys(t *testing.T) {
	snapshot := topKSnapshot{
		Capacity: 2,
		Items: []topKItem{
			{Key: `"alpha"`, Value: "alpha", Count: 1},
			{Key: `"alpha"`, Value: "alpha", Count: 2},
		},
	}
	if err := validateTopKSnapshot(snapshot); err == nil {
		t.Fatal("validateTopKSnapshot(duplicate key) error = nil, want error")
	}
	snapshot.Items = []topKItem{{Key: `"alpha"`, Value: "beta", Count: 1}}
	if err := validateTopKSnapshot(snapshot); err == nil {
		t.Fatal("validateTopKSnapshot(mismatched key) error = nil, want error")
	}
	snapshot = topKSnapshot{
		Capacity: 1,
		Total:    1,
		Items: []topKItem{
			{Key: "fn", Value: func() {}, Count: 1},
		},
	}
	if err := validateTopKSnapshot(snapshot); err == nil {
		t.Fatal("validateTopKSnapshot(unsupported value) error = nil, want error")
	}
}

func TestTopKStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertTopK("top", 10); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	idx := ht.Get("top").Index
	ht.UpsertString("top", "value")
	if !topKIndexReleased(ht, idx) {
		t.Fatalf("overwritten Top-K index %d was not released", idx)
	}

	if err := ht.UpsertTopK("new", 10); err != nil {
		t.Fatalf("UpsertTopK(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Top-K storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestReservoirSampleOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertReservoirSample("sample", 3); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	if hval := ht.Get("sample"); !hval.IsReservoirSample() {
		t.Fatalf("UpsertReservoirSample stored type %+v, want reservoir sample", hval)
	}
	if got := ht.AddReservoirSample("sample", "alpha"); !got.Accepted || got.Seen != 1 || got.Tracked != 1 || got.Capacity != 3 {
		t.Fatalf("AddReservoirSample(alpha) = %#v, want first accepted sample", got)
	}
	update := ht.AddReservoirSample("sample", "beta", "gamma", "delta", "epsilon")
	if update.Seen != 5 || update.Tracked != 3 || update.Capacity != 3 {
		t.Fatalf("AddReservoirSample(batch) = %#v, want bounded sample after five values", update)
	}
	items := ht.GetReservoirSample("sample")
	if len(items) != 3 {
		t.Fatalf("GetReservoirSample len = %d, want capacity 3", len(items))
	}
	for idx := 1; idx < len(items); idx++ {
		if items[idx-1].Priority > items[idx].Priority {
			t.Fatalf("GetReservoirSample items are not priority sorted: %#v", items)
		}
	}
	info, ok := ht.ReservoirSampleInfo("sample")
	if !ok {
		t.Fatal("ReservoirSampleInfo(sample) = false, want true")
	}
	if info.Capacity != 3 || info.Tracked != 3 || info.Seen != 5 || info.EncodedBytes == 0 {
		t.Fatalf("ReservoirSampleInfo(sample) = %#v, want populated bounded sample", info)
	}

	idx := ht.Get("sample").Index
	if err := ht.UpsertReservoirSample("sample", 2); err != nil {
		t.Fatalf("UpsertReservoirSample(replace) error = %v", err)
	}
	if got := ht.Get("sample"); !got.IsReservoirSample() || got.Index != idx {
		t.Fatalf("UpsertReservoirSample replacement stored %+v, want same reservoir slot %d", got, idx)
	}
	if got := ht.GetReservoirSample("sample"); len(got) != 0 {
		t.Fatalf("GetReservoirSample(after replace) = %#v, want empty sample", got)
	}

	if got := ht.AddReservoirSample("auto", "value"); !got.Accepted || got.Seen != 1 || got.Capacity != DefaultReservoirSampleCapacity {
		t.Fatalf("AddReservoirSample(auto) = %#v, want default accepted sample", got)
	}
	if hval := ht.Get("auto"); !hval.IsReservoirSample() {
		t.Fatalf("AddReservoirSample(auto) stored type %+v, want reservoir sample", hval)
	}
}

func TestReservoirSampleClonesNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	value := Map{"path": "/api/users"}
	if got := ht.AddReservoirSample("sample", value); !got.Accepted {
		t.Fatalf("AddReservoirSample() = %#v, want accepted", got)
	}
	value["path"] = "/caller"
	items := ht.GetReservoirSample("sample")
	if got := items[0].Value.(Map)["path"]; got != "/api/users" {
		t.Fatalf("stored reservoir sample value = %v, want cloned /api/users", got)
	}
	items[0].Value.(Map)["path"] = "/mutated"
	if got := ht.GetReservoirSample("sample")[0].Value.(Map)["path"]; got != "/api/users" {
		t.Fatalf("GetReservoirSample exposed nested value: %v", got)
	}
}

func TestReservoirSampleRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if update, err := ht.AddReservoirSampleChecked("sample", "alpha"); err != nil || !update.Accepted {
		t.Fatalf("AddReservoirSampleChecked(alpha) = %#v/%v, want accepted nil error", update, err)
	}

	if update, err := ht.AddReservoirSampleChecked("sample", "beta", func() {}); err == nil {
		t.Fatalf("AddReservoirSampleChecked(unsupported) = %#v/nil, want error", update)
	}
	info, ok := ht.ReservoirSampleInfo("sample")
	if !ok || info.Seen != 1 || info.Tracked != 1 {
		t.Fatalf("ReservoirSampleInfo(after rejected batch) = %#v/%v, want one retained item", info, ok)
	}
	items := ht.GetReservoirSample("sample")
	if len(items) != 1 || items[0].Value != "alpha" {
		t.Fatalf("GetReservoirSample(after rejected batch) = %#v, want alpha only", items)
	}

	if update, err := ht.AddReservoirSampleChecked("missing", func() {}); err == nil {
		t.Fatalf("AddReservoirSampleChecked(missing unsupported) = %#v/nil, want error", update)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key sample left value %+v", got)
	}
	if got := ht.AddReservoirSample("legacy", func() {}); got != (ReservoirSampleUpdate{}) {
		t.Fatalf("AddReservoirSample(unsupported) = %#v, want zero update", got)
	}
	if got := ht.Get("legacy"); !got.Empty() {
		t.Fatalf("legacy rejected sample left value %+v", got)
	}
}

func TestReservoirSampleRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, capacity := range []uint64{0, maxReservoirSampleCapacity + 1} {
		if err := ht.UpsertReservoirSample("bad", capacity); err == nil {
			t.Fatalf("UpsertReservoirSample(%d) error = nil, want error", capacity)
		}
		if got := ht.Get("bad"); !got.Empty() {
			t.Fatalf("invalid reservoir sample config stored value %+v", got)
		}
	}
}

func TestReservoirSampleSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	sample, err := newReservoirSampleData(2)
	if err != nil {
		t.Fatalf("newReservoirSampleData() error = %v", err)
	}
	sample.AddOne("alpha", "beta")
	snapshot := sample.Snapshot()
	snapshot.Items[0].Priority++
	if err := validateReservoirSampleSnapshot(snapshot); err == nil {
		t.Fatal("validateReservoirSampleSnapshot(mismatched priority) error = nil, want error")
	}

	snapshot = sample.Snapshot()
	snapshot.Items[1].Sequence = snapshot.Items[0].Sequence
	if err := validateReservoirSampleSnapshot(snapshot); err == nil {
		t.Fatal("validateReservoirSampleSnapshot(duplicate sequence) error = nil, want error")
	}

	snapshot = sample.Snapshot()
	snapshot.Seen = 1
	if err := validateReservoirSampleSnapshot(snapshot); err == nil {
		t.Fatal("validateReservoirSampleSnapshot(seen below tracked) error = nil, want error")
	}

	snapshot = reservoirSampleSnapshot{
		Capacity: 1,
		Seen:     1,
		Items: []reservoirSampleItem{
			{Value: func() {}, Priority: 1, Sequence: 1},
		},
	}
	if err := validateReservoirSampleSnapshot(snapshot); err == nil {
		t.Fatal("validateReservoirSampleSnapshot(unsupported value) error = nil, want error")
	}
}

func TestReservoirSampleStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertReservoirSample("sample", 10); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	idx := ht.Get("sample").Index
	ht.UpsertString("sample", "value")
	if !reservoirSampleIndexReleased(ht, idx) {
		t.Fatalf("overwritten reservoir sample index %d was not released", idx)
	}

	if err := ht.UpsertReservoirSample("new", 10); err != nil {
		t.Fatalf("UpsertReservoirSample(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("reservoir sample storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestQuantileSketchOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertQuantileSketch("latency", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	if hval := ht.Get("latency"); !hval.IsQuantileSketch() {
		t.Fatalf("UpsertQuantileSketch stored type %+v, want quantile sketch", hval)
	}
	values := make([]float64, 0, 100)
	for idx := 1; idx <= 100; idx++ {
		values = append(values, float64(idx))
	}
	if estimate := ht.AddQuantileSketch("latency", values[0], values[1:]...); estimate.Count != 100 {
		t.Fatalf("AddQuantileSketch() estimate = %#v, want count 100", estimate)
	}
	p50, ok := ht.EstimateQuantileSketch("latency", 0.5)
	if !ok || p50.Value < 45 || p50.Value > 55 {
		t.Fatalf("EstimateQuantileSketch(p50) = %#v/%v, want around median", p50, ok)
	}
	p95, ok := ht.EstimateQuantileSketch("latency", 0.95)
	if !ok || p95.Value < 90 || p95.Value > 100 {
		t.Fatalf("EstimateQuantileSketch(p95) = %#v/%v, want upper tail", p95, ok)
	}
	info, ok := ht.QuantileSketchInfo("latency")
	if !ok {
		t.Fatal("QuantileSketchInfo(latency) = false, want true")
	}
	if info.Epsilon != 0.01 || info.Count != 100 || info.SummarySize == 0 || info.SummarySize >= info.Count || info.Min != 1 || info.Max != 100 || info.EncodedBytes <= 0 {
		t.Fatalf("QuantileSketchInfo(latency) = %#v, want compact populated sketch", info)
	}

	if err := ht.UpsertQuantileSketch("latency", 0.02); err != nil {
		t.Fatalf("UpsertQuantileSketch(replace) error = %v", err)
	}
	if info, ok := ht.QuantileSketchInfo("latency"); !ok || info.Count != 0 || info.Epsilon != 0.02 {
		t.Fatalf("QuantileSketchInfo(after replace) = %#v/%v, want empty replacement", info, ok)
	}
	if got := ht.AddQuantileSketch("auto", 42); got.Count != 1 || got.Value != 42 {
		t.Fatalf("AddQuantileSketch(auto) = %#v, want one observed value", got)
	}
	if hval := ht.Get("auto"); !hval.IsQuantileSketch() {
		t.Fatalf("AddQuantileSketch(auto) stored type %+v, want quantile sketch", hval)
	}
	if estimate, ok := ht.EstimateQuantileSketch("latency", 1.1); ok || estimate.Count != 0 {
		t.Fatalf("EstimateQuantileSketch(invalid quantile) = %#v/%v, want false", estimate, ok)
	}
	if got := ht.AddQuantileSketch("bad-value", math.NaN()); got.Count != 0 {
		t.Fatalf("AddQuantileSketch(NaN) = %#v, want empty estimate", got)
	}
	if hval := ht.Get("bad-value"); !hval.Empty() {
		t.Fatalf("invalid quantile sketch value created key %+v", hval)
	}
}

func TestQuantileSketchRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, epsilon := range []float64{0, minQuantileSketchEpsilon / 2, maxQuantileSketchEpsilon + 0.01, math.NaN(), math.Inf(1)} {
		if err := ht.UpsertQuantileSketch("bad", epsilon); err == nil {
			t.Fatalf("UpsertQuantileSketch(%v) error = nil, want error", epsilon)
		}
		if got := ht.Get("bad"); !got.Empty() {
			t.Fatalf("invalid quantile sketch config stored value %+v", got)
		}
	}
}

func TestQuantileSketchSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	sketch, err := newQuantileSketchData(0.01)
	if err != nil {
		t.Fatalf("newQuantileSketchData() error = %v", err)
	}
	sketch.Add(1, 2, 3)
	snapshot := sketch.Snapshot()

	unsorted := snapshot
	unsorted.Summary = append([]quantileSketchSample(nil), snapshot.Summary...)
	unsorted.Summary[1].Value = unsorted.Summary[0].Value - 1
	if err := validateQuantileSketchSnapshot(unsorted); err == nil {
		t.Fatal("validateQuantileSketchSnapshot(unsorted) error = nil, want error")
	}

	badGap := snapshot
	badGap.Summary = append([]quantileSketchSample(nil), snapshot.Summary...)
	badGap.Summary[0].Gap = 0
	if err := validateQuantileSketchSnapshot(badGap); err == nil {
		t.Fatal("validateQuantileSketchSnapshot(zero gap) error = nil, want error")
	}

	nonFinite := snapshot
	nonFinite.Summary = append([]quantileSketchSample(nil), snapshot.Summary...)
	nonFinite.Summary[0].Value = math.Inf(1)
	if err := validateQuantileSketchSnapshot(nonFinite); err == nil {
		t.Fatal("validateQuantileSketchSnapshot(non-finite value) error = nil, want error")
	}
}

func TestQuantileSketchStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertQuantileSketch("latency", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	idx := ht.Get("latency").Index
	ht.UpsertString("latency", "value")
	if !quantileSketchIndexReleased(ht, idx) {
		t.Fatalf("overwritten quantile sketch index %d was not released", idx)
	}

	if err := ht.UpsertQuantileSketch("new", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("quantile sketch storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestFenwickTreeOperations(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertFenwickTree("scores", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if hval := ht.Get("scores"); !hval.IsFenwickTree() {
		t.Fatalf("UpsertFenwickTree stored type %+v, want fenwick tree", hval)
	}
	if update, ok := ht.AddFenwickTree("scores", 0, 5); !ok || update.Value != 5 || update.PrefixSum != 5 || update.Total != 5 {
		t.Fatalf("AddFenwickTree(index 0) = %#v/%v, want first update", update, ok)
	}
	if update, ok := ht.AddFenwickTree("scores", 3, 7); !ok || update.Value != 7 || update.PrefixSum != 12 || update.Total != 12 {
		t.Fatalf("AddFenwickTree(index 3) = %#v/%v, want prefix 12", update, ok)
	}
	if update, ok := ht.AddFenwickTree("scores", 3, -2); !ok || update.Value != 5 || update.PrefixSum != 10 || update.Total != 10 || update.Updates != 3 {
		t.Fatalf("AddFenwickTree(negative delta) = %#v/%v, want adjusted value", update, ok)
	}
	if got, ok := ht.GetFenwickTree("scores", 3); !ok || got != 5 {
		t.Fatalf("GetFenwickTree(3) = %d/%v, want 5", got, ok)
	}
	if got, ok := ht.PrefixSumFenwickTree("scores", 2); !ok || got != 5 {
		t.Fatalf("PrefixSumFenwickTree(2) = %d/%v, want 5", got, ok)
	}
	if got, ok := ht.RangeSumFenwickTree("scores", 1, 3); !ok || got != 5 {
		t.Fatalf("RangeSumFenwickTree(1, 3) = %d/%v, want 5", got, ok)
	}
	info, ok := ht.FenwickTreeInfo("scores")
	if !ok {
		t.Fatal("FenwickTreeInfo(scores) = false, want true")
	}
	if info.Size != 8 || info.Updates != 3 || info.Total != 10 || info.TreeBytes != 72 || info.EncodedBytes <= 0 {
		t.Fatalf("FenwickTreeInfo(scores) = %#v, want compact populated tree", info)
	}

	if err := ht.UpsertFenwickTree("scores", 4); err != nil {
		t.Fatalf("UpsertFenwickTree(replace) error = %v", err)
	}
	if info, ok := ht.FenwickTreeInfo("scores"); !ok || info.Size != 4 || info.Total != 0 || info.Updates != 0 {
		t.Fatalf("FenwickTreeInfo(after replace) = %#v/%v, want empty replacement", info, ok)
	}
	if update, ok := ht.AddFenwickTree("auto", 2, 9); !ok || update.Value != 9 || update.PrefixSum != 9 {
		t.Fatalf("AddFenwickTree(auto) = %#v/%v, want created default tree", update, ok)
	}
	if hval := ht.Get("auto"); !hval.IsFenwickTree() {
		t.Fatalf("AddFenwickTree(auto) stored type %+v, want fenwick tree", hval)
	}
	if update, ok := ht.AddFenwickTree("noop", 0, 0); ok || update.Total != 0 {
		t.Fatalf("AddFenwickTree(zero delta) = %#v/%v, want false", update, ok)
	}
	if hval := ht.Get("noop"); !hval.Empty() {
		t.Fatalf("zero-delta Fenwick update created key %+v", hval)
	}
}

func TestFenwickTreeRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, size := range []uint64{0, maxFenwickTreeSize + 1} {
		if err := ht.UpsertFenwickTree("bad", size); err == nil {
			t.Fatalf("UpsertFenwickTree(%d) error = nil, want error", size)
		}
		if got := ht.Get("bad"); !got.Empty() {
			t.Fatalf("invalid Fenwick tree config stored value %+v", got)
		}
	}
}

func TestFenwickTreeRejectsOverflowAndInvalidUpdates(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertFenwickTree("scores", 4); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := ht.AddFenwickTree("scores", 4, 1); ok {
		t.Fatal("AddFenwickTree(out of range) ok = true, want false")
	}
	if update, ok := ht.AddFenwickTree("scores", 1, maxFenwickTreeInt64); !ok || update.Total != maxFenwickTreeInt64 {
		t.Fatalf("AddFenwickTree(max) = %#v/%v, want max total", update, ok)
	}
	if _, ok := ht.AddFenwickTree("scores", 1, 1); ok {
		t.Fatal("AddFenwickTree(point overflow) ok = true, want false")
	}
	if got, ok := ht.GetFenwickTree("scores", 1); !ok || got != maxFenwickTreeInt64 {
		t.Fatalf("GetFenwickTree(after overflow) = %d/%v, want unchanged max", got, ok)
	}

	if err := ht.UpsertFenwickTree("negative", 2); err != nil {
		t.Fatalf("UpsertFenwickTree(negative) error = %v", err)
	}
	if update, ok := ht.AddFenwickTree("negative", 0, minFenwickTreeInt64); !ok || update.Total != minFenwickTreeInt64 {
		t.Fatalf("AddFenwickTree(min) = %#v/%v, want min total", update, ok)
	}
	if _, ok := ht.AddFenwickTree("negative", 0, -1); ok {
		t.Fatal("AddFenwickTree(negative overflow) ok = true, want false")
	}
}

func TestFenwickTreeSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	tree, err := newFenwickTreeData(4)
	if err != nil {
		t.Fatalf("newFenwickTreeData() error = %v", err)
	}
	tree.Add(0, 5)
	tree.Add(3, 7)
	snapshot := tree.Snapshot()

	badLength := snapshot
	badLength.Tree = append([]int64(nil), snapshot.Tree[:len(snapshot.Tree)-1]...)
	if err := validateFenwickTreeSnapshot(badLength); err == nil {
		t.Fatal("validateFenwickTreeSnapshot(bad length) error = nil, want error")
	}

	badSentinel := snapshot
	badSentinel.Tree = append([]int64(nil), snapshot.Tree...)
	badSentinel.Tree[0] = 1
	if err := validateFenwickTreeSnapshot(badSentinel); err == nil {
		t.Fatal("validateFenwickTreeSnapshot(bad sentinel) error = nil, want error")
	}

	badTotal := snapshot
	badTotal.Total++
	if err := validateFenwickTreeSnapshot(badTotal); err == nil {
		t.Fatal("validateFenwickTreeSnapshot(bad total) error = nil, want error")
	}
}

func TestFenwickTreeStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)

	if err := ht.UpsertFenwickTree("scores", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	idx := ht.Get("scores").Index
	ht.UpsertString("scores", "value")
	if !fenwickTreeIndexReleased(ht, idx) {
		t.Fatalf("overwritten Fenwick tree index %d was not released", idx)
	}

	if err := ht.UpsertFenwickTree("new", 8); err != nil {
		t.Fatalf("UpsertFenwickTree(new) error = %v", err)
	}
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("Fenwick tree storage was not reused: got index %d, want %d", got, idx)
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
	if !rawIndexReleased(ht, idx) {
		t.Fatalf("deleted raw index %d was not released", idx)
	}

	ht.UpsertString("new", "value")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("raw storage was not reused: got index %d, want %d", got, idx)
	}
}

func TestDelAliasDeletesAndReleasesBackingStorage(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertBytes("key", []byte("value"))
	idx := ht.Get("key").Index
	ht.Del("key")
	if got := ht.Get("key"); !got.Empty() {
		t.Fatalf("Del(key) left value present: %+v", got)
	}
	if !rawIndexReleased(ht, idx) {
		t.Fatalf("Del(key) did not release raw index %d", idx)
	}
}

func TestReusableIndexesDeduplicateAndSkipStaleEntries(t *testing.T) {
	var indexes reusableIndexes
	if indexes.Len() != 0 {
		t.Fatalf("empty Len() = %d, want 0", indexes.Len())
	}
	if !indexes.Mark(3) {
		t.Fatal("Mark(3) = false, want true")
	}
	if indexes.Mark(3) {
		t.Fatal("duplicate Mark(3) = true, want false")
	}
	if !indexes.Has(3) || indexes.Len() != 1 {
		t.Fatalf("after Mark(3): has=%v len=%d, want has true len 1", indexes.Has(3), indexes.Len())
	}
	if !indexes.Use(3) {
		t.Fatal("Use(3) = false, want true")
	}
	if indexes.Has(3) || indexes.Len() != 0 {
		t.Fatalf("after Use(3): has=%v len=%d, want has false len 0", indexes.Has(3), indexes.Len())
	}

	indexes.Mark(1)
	indexes.Mark(2)
	indexes.Use(2)
	idx, ok := indexes.Take()
	if !ok || idx != 1 {
		t.Fatalf("Take() = %d/%v, want 1/true after stale index", idx, ok)
	}
	if _, ok := indexes.Take(); ok {
		t.Fatal("second Take() ok = true, want false")
	}
}

func TestTrimReusableTailCompactsReusableIndexMetadata(t *testing.T) {
	values := make([]int, 200)
	var indexes reusableIndexes
	indexes.Mark(10)
	for idx := int32(64); idx < int32(len(values)); idx++ {
		indexes.Mark(idx)
	}

	values = trimReusableTail(values, &indexes)
	if got := len(values); got != 64 {
		t.Fatalf("trimmed values len = %d, want 64", got)
	}
	if got := indexes.Len(); got != 1 {
		t.Fatalf("reusable count after trim = %d, want 1", got)
	}
	if got := len(indexes.stack); got != 1 {
		t.Fatalf("reusable stack len after trim = %d, want 1", got)
	}
	if got := len(indexes.bits); got != 1 {
		t.Fatalf("reusable bitmap words after trim = %d, want 1", got)
	}
	if !indexes.Has(10) {
		t.Fatal("remaining reusable index 10 was lost")
	}
	if indexes.Has(64) {
		t.Fatal("trimmed reusable index 64 still marked")
	}
	if got, ok := indexes.Take(); !ok || got != 10 {
		t.Fatalf("Take() after trim = %d/%v, want 10/true", got, ok)
	}
}

func TestBytesStorageClonesCallerOwnedValues(t *testing.T) {
	store := CreateBytesStorage()
	value := []byte("value")
	idx := store.Add(value)
	value[0] = 'X'
	if got := string(store.array[idx]); got != "value" {
		t.Fatalf("Add() stored caller-owned bytes: %q", got)
	}

	replacement := []byte("next")
	store.Put(idx, replacement)
	replacement[0] = 'X'
	if got := string(store.array[idx]); got != "next" {
		t.Fatalf("Put() stored caller-owned bytes: %q", got)
	}
}

func TestStoragePoolsTrimReusableTailSlots(t *testing.T) {
	raws := CreateBytesStorage()
	raws.Add([]byte("zero"))
	rawMiddle := raws.Add([]byte("middle"))
	rawTail := raws.Add([]byte("tail"))
	raws.Del(rawMiddle)
	if got := len(raws.array); got != 3 {
		t.Fatalf("raw len after middle delete = %d, want 3", got)
	}
	if got := raws.Add([]byte("reuse-middle")); got != rawMiddle {
		t.Fatalf("raw Add() after middle delete = %d, want reused index %d", got, rawMiddle)
	}
	raws.Del(rawMiddle)
	raws.Del(rawTail)
	if got := len(raws.array); got != 1 {
		t.Fatalf("raw len after tail trim = %d, want 1", got)
	}
	if got := raws.Add([]byte("next")); got != 1 {
		t.Fatalf("raw Add() after trim = %d, want appended index 1", got)
	}

	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	diskMiddle, err := disks.Add([]byte("middle"))
	if err != nil {
		t.Fatalf("disk Add(middle) error = %v", err)
	}
	diskTail, err := disks.Add([]byte("tail"))
	if err != nil {
		t.Fatalf("disk Add(tail) error = %v", err)
	}
	disks.Del(diskMiddle)
	if got := len(disks.paths); got != 2 {
		t.Fatalf("disk len after middle delete = %d, want 2", got)
	}
	disks.Del(diskTail)
	if got := len(disks.paths); got != 0 {
		t.Fatalf("disk len after tail trim = %d, want 0", got)
	}
	if got, err := disks.Add([]byte("next")); err != nil || got != 0 {
		t.Fatalf("disk Add() after trim = %d/%v, want 0/nil", got, err)
	}

	maps := CreateMapStorage()
	maps.Add(Map{"zero": true})
	mapMiddle := maps.Add(Map{"middle": true})
	mapTail := maps.Add(Map{"tail": true})
	maps.Del(mapMiddle)
	maps.Del(mapTail)
	if got := len(maps.array); got != 1 {
		t.Fatalf("map len after tail trim = %d, want 1", got)
	}

	slices := CreateSliceStorage()
	slices.Add(Slice{"zero"})
	sliceMiddle := slices.Add(Slice{"middle"})
	sliceTail := slices.Add(Slice{"tail"})
	slices.Del(sliceMiddle)
	slices.Del(sliceTail)
	if got := len(slices.array); got != 1 {
		t.Fatalf("slice len after tail trim = %d, want 1", got)
	}

	sets := CreateSetStorage()
	sets.Add(Set{"zero"})
	setMiddle := sets.Add(Set{"middle"})
	setTail := sets.Add(Set{"tail"})
	sets.Del(setMiddle)
	sets.Del(setTail)
	if got := len(sets.array); got != 1 {
		t.Fatalf("set len after tail trim = %d, want 1", got)
	}

	refs := CreateLevelDBReferenceStorage()
	refs.Add(LevelDBReference{Key: "zero"})
	refMiddle := refs.Add(LevelDBReference{Key: "middle"})
	refTail := refs.Add(LevelDBReference{Key: "tail"})
	refs.Del(refMiddle)
	refs.Del(refTail)
	if got := len(refs.array); got != 1 {
		t.Fatalf("leveldb ref len after tail trim = %d, want 1", got)
	}

	queues := CreatePriorityQueueStorage()
	queues.Add(PriorityQueue{{Priority: 1, Value: "zero"}})
	queueMiddle := queues.Add(PriorityQueue{{Priority: 1, Value: "middle"}})
	queueTail := queues.Add(PriorityQueue{{Priority: 1, Value: "tail"}})
	queues.Del(queueMiddle)
	queues.Del(queueTail)
	if got := len(queues.array); got != 1 {
		t.Fatalf("priority queue len after tail trim = %d, want 1", got)
	}
}

func TestDiskStorageWritesReplaceAndReuseAtomically(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}

	idx, err := disks.Add([]byte("first"))
	if err != nil {
		t.Fatalf("Add(first) error = %v", err)
	}
	if err := disks.Put(idx, []byte("second")); err != nil {
		t.Fatalf("Put(second) error = %v", err)
	}
	if got, err := disks.Get(idx); err != nil || string(got) != "second" {
		t.Fatalf("Get(after Put) = %q/%v, want second/nil", got, err)
	}

	disks.Del(idx)
	reused, err := disks.Add([]byte("third"))
	if err != nil {
		t.Fatalf("Add(third) error = %v", err)
	}
	if reused != idx {
		t.Fatalf("Add(third) index = %d, want reused %d", reused, idx)
	}
	if got, err := disks.Get(reused); err != nil || string(got) != "third" {
		t.Fatalf("Get(reused) = %q/%v, want third/nil", got, err)
	}
	assertNoAtomicTempFiles(t, disks.dir, filepath.Base(disks.paths[reused]))
}

func TestDiskStorageDeleteClearsMiddlePathAndRestoresOnReuse(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}

	if _, err := disks.Add([]byte("zero")); err != nil {
		t.Fatalf("Add(zero) error = %v", err)
	}
	middle, err := disks.Add([]byte("middle"))
	if err != nil {
		t.Fatalf("Add(middle) error = %v", err)
	}
	if _, err := disks.Add([]byte("tail")); err != nil {
		t.Fatalf("Add(tail) error = %v", err)
	}
	middlePath := disks.paths[middle]

	disks.Del(middle)

	if got := disks.paths[middle]; got != "" {
		t.Fatalf("deleted middle path = %q, want empty", got)
	}
	if _, err := os.Stat(middlePath); !os.IsNotExist(err) {
		t.Fatalf("deleted middle file Stat() error = %v, want not exist", err)
	}

	reused, err := disks.Add([]byte("reused"))
	if err != nil {
		t.Fatalf("Add(reused) error = %v", err)
	}
	if reused != middle {
		t.Fatalf("Add(reused) index = %d, want middle index %d", reused, middle)
	}
	if got := disks.paths[reused]; got != middlePath {
		t.Fatalf("reused path = %q, want %q", got, middlePath)
	}
	if got, err := disks.Get(reused); err != nil || string(got) != "reused" {
		t.Fatalf("Get(reused) = %q/%v, want reused/nil", got, err)
	}
}

func TestDiskStorageWriteFailureCleansTemporaryFilesAndKeepsReusableIndex(t *testing.T) {
	dir := t.TempDir()
	disks, err := CreateDiskStorage(dir, false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	badPath := filepath.Join(dir, "bytes-0.bin")
	if err := os.Mkdir(badPath, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	disks.paths = append(disks.paths, badPath)
	disks.reusables.Mark(0)

	if _, err := disks.Add([]byte("payload")); err == nil {
		t.Fatal("Add(payload) error = nil, want write error")
	}
	if !disks.reusables.Has(0) {
		t.Fatal("failed Add did not restore reusable disk index")
	}
	assertNoAtomicTempFiles(t, dir, "bytes-0.bin")
}

func TestTypeReplacementReleasesPreviousStorage(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertMap("key", Map{"old": true})
	mapIdx := ht.Get("key").Index
	ht.UpsertSlice("key", Slice{"new"})

	if hval := ht.Get("key"); !hval.IsSlice() {
		t.Fatalf("replacement type = %+v, want slice", hval)
	}
	if !mapIndexReleased(ht, mapIdx) {
		t.Fatalf("replaced map index %d was not released", mapIdx)
	}

	sliceIdx := ht.Get("key").Index
	ht.UpsertPriorityQueue("key", PriorityQueue{{Priority: 1, Value: "job"}})
	if hval := ht.Get("key"); !hval.IsPriorityQueue() {
		t.Fatalf("replacement type = %+v, want priority queue", hval)
	}
	if !sliceIndexReleased(ht, sliceIdx) {
		t.Fatalf("replaced slice index %d was not released", sliceIdx)
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
	if !rawIndexReleased(ht, idx) {
		t.Fatalf("expired raw index %d was not released", idx)
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

func TestExpireAtPastDeletesImmediatelyAndReusesStorage(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(250, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertBytes("key", []byte("value"))
	idx := ht.Get("key").Index
	if !ht.ExpireAt("key", now) {
		t.Fatal("ExpireAt(key, now) = false, want true")
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("size after immediate ExpireAt = %d, want 0", got)
	}
	if !rawIndexReleased(ht, idx) {
		t.Fatalf("expired raw index %d was not released immediately", idx)
	}

	ht.UpsertBytes("next", []byte("value"))
	if got := ht.Get("next").Index; got != idx {
		t.Fatalf("raw storage after immediate ExpireAt = %d, want reused %d", got, idx)
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
	ht.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 1, Value: "value"}})
	for _, key := range []string{"counter", "bytes", "map", "slice", "priority"} {
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
	if got, ok := ht.PeekPriorityQueue("priority"); ok || got.Value != nil {
		t.Fatalf("expired priority queue peek = %#v/%v, want missing", got, ok)
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
	ht.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 1, Value: "item"}})

	wantKeys := []string{"apple", "apricot", "banana", "map", "priority", "slice"}
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
	if got := values["priority"]; !got.IsPriorityQueue() {
		t.Fatalf("priority entry = %+v, want priority queue", got)
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
	if !rawIndexReleased(ht, expiredIdx) {
		t.Fatalf("expired raw index %d was not released", expiredIdx)
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
	if !rawIndexReleased(ht, expiredRawIdx) {
		t.Fatalf("expired raw index %d was not released", expiredRawIdx)
	}
	if !mapIndexReleased(ht, expiredMapIdx) {
		t.Fatalf("expired map index %d was not released", expiredMapIdx)
	}
	if got := ht.VacuumExpired(); got != 0 {
		t.Fatalf("second VacuumExpired() = %d, want 0", got)
	}
}

func TestVacuumExpiredSkipsStaleExpirationEntries(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(750, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("extended", "value")
	if !ht.Expire("extended", 10*time.Second) {
		t.Fatal("first Expire(extended) = false, want true")
	}
	if !ht.Expire("extended", time.Hour) {
		t.Fatal("second Expire(extended) = false, want true")
	}

	now = now.Add(11 * time.Second)
	if got := ht.VacuumExpired(); got != 0 {
		t.Fatalf("VacuumExpired() before latest deadline = %d, want 0", got)
	}
	if got := ht.GetString("extended"); got != "value" {
		t.Fatalf("extended value after stale deadline = %q, want value", got)
	}

	now = now.Add(time.Hour)
	if got := ht.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() after latest deadline = %d, want 1", got)
	}
	if got := ht.GetString("extended"); got != "" {
		t.Fatalf("extended value after latest deadline = %q, want empty", got)
	}
}

func TestExpirationHeapCompactsStaleEntries(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(760, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("hot", "value")
	for i := 0; i < 128; i++ {
		if !ht.ExpireAt("hot", now.Add(time.Duration(i+1)*time.Hour)) {
			t.Fatalf("ExpireAt(hot, %d) = false, want true", i)
		}
	}
	if got := ht.expirations.Len(); got > 64 {
		t.Fatalf("expiration heap len after repeated updates = %d, want compacted <= 64", got)
	}
	if !ht.Persist("hot") {
		t.Fatal("Persist(hot) = false, want true")
	}
	if got := ht.expirations.Len(); got != 0 {
		t.Fatalf("expiration heap len after Persist() = %d, want 0", got)
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

func TestVacuumExpiredOnMemoryPressure(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(850, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("expired", "value")
	if !ht.Expire("expired", time.Second) {
		t.Fatal("Expire(expired) = false, want true")
	}
	now = now.Add(2 * time.Second)

	if got := ht.VacuumExpiredOnMemoryPressure(^uint64(0)); got != 0 {
		t.Fatalf("below-threshold VacuumExpiredOnMemoryPressure() = %d, want 0", got)
	}
	if got := ht.Size(); got != 1 {
		t.Fatalf("size after below-threshold vacuum = %d, want 1", got)
	}
	if got := ht.VacuumExpiredOnMemoryPressure(1); got != 1 {
		t.Fatalf("pressure VacuumExpiredOnMemoryPressure() = %d, want 1", got)
	}
	if got := ht.Size(); got != 0 {
		t.Fatalf("size after pressure vacuum = %d, want 0", got)
	}
}

func TestStartMemoryPressureVacuumRemovesExpiredKeysAndStops(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(860, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("key", "value")
	if !ht.Expire("key", time.Second) {
		t.Fatal("Expire(key) = false, want true")
	}
	now = now.Add(2 * time.Second)

	stop := ht.StartMemoryPressureVacuum(time.Millisecond, 1)
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
		t.Fatal("memory pressure vacuum stop did not return")
	}
}

func TestMemoryPressureVacuumRejectsInvalidConfig(t *testing.T) {
	ht := newTestTrie(t)

	for _, test := range []struct {
		name string
		fn   func()
	}{
		{name: "immediate threshold", fn: func() { ht.VacuumExpiredOnMemoryPressure(0) }},
		{name: "cleaner interval", fn: func() { ht.StartMemoryPressureVacuum(0, 1) }},
		{name: "cleaner threshold", fn: func() { ht.StartMemoryPressureVacuum(time.Millisecond, 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("panic = nil, want panic")
				}
			}()
			test.fn()
		})
	}
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
	if err := ht.UpsertQuantileSketch("latency", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	ht.AddQuantileSketch("latency", 10, 20, 30)
	if err := ht.UpsertFenwickTree("scores", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	ht.AddFenwickTree("scores", 2, 7)
	ht.UpsertSparseBitset("ids")
	ht.AddSparseBitset("ids", 1, ^uint64(0))
	if err := ht.UpsertReservoirSample("sample", 8); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	ht.AddReservoirSample("sample", "alpha", "beta")
	ht.UpsertRadixTree("radix")
	ht.PutRadixTree("radix", "user:100", "active")
	ht.Destroy()
	ht.Destroy()
	if ht.root != nil || ht.raws != nil || ht.disks != nil || ht.maps != nil || ht.slices != nil || ht.sets != nil || ht.priorityQueues != nil || ht.bloomFilters != nil || ht.countMinSketches != nil || ht.hyperLogLogs != nil || ht.topKs != nil || ht.cuckooFilters != nil || ht.roaringBitmaps != nil || ht.quantileSketches != nil || ht.fenwickTrees != nil || ht.sparseBitsets != nil || ht.reservoirSamples != nil || ht.xorFilters != nil || ht.radixTrees != nil || ht.dbrefs != nil || ht.expires != nil || ht.expirations != nil || ht.keyStats != nil || ht.now != nil {
		t.Fatalf("Destroy retained backing state: %+v", ht)
	}

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
