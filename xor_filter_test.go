package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)

var (
	benchmarkXorFilterFingerprintsSink []uint8
	benchmarkXorFilterSnapshotSink     xorFilterSnapshot
	benchmarkXorFilterAttemptSink      int
)

func TestXorFilterPlainJSONStringFastPathMatchesGeneric(t *testing.T) {
	values := []string{"", "alpha", "with space", "punctuation-_.:/"}
	seeds := []uint64{0, 1, xorFilterSeedBase, ^uint64(0)}

	generic, err := newXorFilterData(uint64(len(values)))
	if err != nil {
		t.Fatalf("newXorFilterData(generic) error = %v", err)
	}
	fast, err := newXorFilterData(uint64(len(values)))
	if err != nil {
		t.Fatalf("newXorFilterData(fast) error = %v", err)
	}
	for _, value := range values {
		key, err := xorFilterItemKey(value)
		if err != nil {
			t.Fatalf("xorFilterItemKey(%q) error = %v", value, err)
		}
		if got := xorFilterJSONStringKey(value); got != key {
			t.Fatalf("xorFilterJSONStringKey(%q) = %q, want %q", value, got, key)
		}
		for _, seed := range seeds {
			if got, want := xorFilterHashJSONString(value, seed), xorFilterHashString(key, seed); got != want {
				t.Fatalf("xorFilterHashJSONString(%q, %d) = %d, want %d", value, seed, got, want)
			}
		}
		if _, err := generic.AddOne(value); err != nil {
			t.Fatalf("generic AddOne(%q) error = %v", value, err)
		}
		added, err := fast.addJSONString(value)
		if err != nil || !added {
			t.Fatalf("fast addJSONString(%q) = %v/%v, want true/nil", value, added, err)
		}
	}
	if added, err := fast.addJSONString(values[0]); err != nil || added {
		t.Fatalf("duplicate addJSONString() = %v/%v, want false/nil", added, err)
	}
	if got, want := fast.Snapshot(), generic.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending fast snapshot = %#v, want %#v", got, want)
	}
	if err := generic.Build(); err != nil {
		t.Fatalf("generic Build() error = %v", err)
	}
	if err := fast.Build(); err != nil {
		t.Fatalf("fast Build() error = %v", err)
	}
	if got, want := fast.Snapshot(), generic.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("built fast snapshot = %#v, want %#v", got, want)
	}
	for _, value := range values {
		gotHit, gotQueryable := fast.containsJSONString(value)
		wantHit, wantQueryable, err := generic.ContainsChecked(value)
		if err != nil {
			t.Fatalf("ContainsChecked(%q) error = %v", value, err)
		}
		if gotHit != wantHit || gotQueryable != wantQueryable {
			t.Fatalf("containsJSONString(%q) = %v/%v, want %v/%v", value, gotHit, gotQueryable, wantHit, wantQueryable)
		}
	}
}

func TestXorFilterBuildMatchesFirstSuccessfulFingerprintAttempt(t *testing.T) {
	filter, err := newXorFilterData(64)
	if err != nil {
		t.Fatalf("newXorFilterData() error = %v", err)
	}
	keys := make([]string, 0, 64)
	for item := 0; item < 64; item++ {
		value := fmt.Sprintf("value-%d", item)
		if added, err := filter.addJSONString(value); err != nil || !added {
			t.Fatalf("addJSONString(%q) = %v/%v, want true/nil", value, added, err)
		}
		keys = append(keys, xorFilterJSONStringKey(value))
	}
	sort.Strings(keys)

	var (
		wantFingerprints []uint8
		wantBlockLength  uint32
		wantSeed         uint64
		wantAttempt      = -1
	)
	for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
		seed := xorFilterSeed(len(keys), attempt)
		fingerprints, blockLength, ok := buildXorFilterFingerprintsSliceQueueControl(keys, seed)
		if !ok {
			if fingerprints != nil {
				t.Fatalf("attempt %d returned failed non-nil fingerprints", attempt)
			}
			continue
		}
		wantFingerprints = fingerprints
		wantBlockLength = blockLength
		wantSeed = seed
		wantAttempt = attempt
		break
	}
	if wantAttempt < 0 {
		t.Fatal("reference fingerprint attempts did not find a valid build")
	}
	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if filter.seed != wantSeed || filter.blockLength != wantBlockLength || !reflect.DeepEqual(filter.fingerprints, wantFingerprints) {
		t.Fatalf("Build() state = seed %d block %d fingerprints %v, want attempt %d seed %d block %d fingerprints %v", filter.seed, filter.blockLength, filter.fingerprints, wantAttempt, wantSeed, wantBlockLength, wantFingerprints)
	}
}

func TestXorFilterBuildRetriesWithoutChangingFingerprintResult(t *testing.T) {
	filter, err := newXorFilterData(3)
	if err != nil {
		t.Fatalf("newXorFilterData() error = %v", err)
	}
	keys := make([]string, 0, 3)
	for item := 0; item < 3; item++ {
		value := fmt.Sprintf("retry-45-value-%d", item)
		if added, err := filter.addJSONString(value); err != nil || !added {
			t.Fatalf("addJSONString(%q) = %v/%v, want true/nil", value, added, err)
		}
		keys = append(keys, xorFilterJSONStringKey(value))
	}
	sort.Strings(keys)
	if fingerprints, _, ok := buildXorFilterFingerprintsSliceQueueControl(keys, xorFilterSeed(len(keys), 0)); ok || fingerprints != nil {
		t.Fatalf("attempt 0 = %v/%v, want failed nil fingerprints", ok, fingerprints)
	}
	wantFingerprints, wantBlockLength, ok := buildXorFilterFingerprintsSliceQueueControl(keys, xorFilterSeed(len(keys), 1))
	if !ok {
		t.Fatal("attempt 1 failed, want deterministic successful retry")
	}
	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if filter.seed != xorFilterSeed(len(keys), 1) || filter.blockLength != wantBlockLength || !reflect.DeepEqual(filter.fingerprints, wantFingerprints) {
		t.Fatalf("Build() state = seed %d block %d fingerprints %v, want retry seed %d block %d fingerprints %v", filter.seed, filter.blockLength, filter.fingerprints, xorFilterSeed(len(keys), 1), wantBlockLength, wantFingerprints)
	}
}

func TestXorFilterFingerprintBuildIsOrderIndependent(t *testing.T) {
	keys := make([]string, 4096)
	for idx := range keys {
		keys[idx] = xorFilterJSONStringKey(fmt.Sprintf("order-%d", idx))
	}
	sort.Strings(keys)
	reversed := append([]string(nil), keys...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}

	for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
		seed := xorFilterSeed(len(keys), attempt)
		orderedFingerprints, orderedBlockLength, orderedOK := buildXorFilterFingerprints(keys, seed)
		reversedFingerprints, reversedBlockLength, reversedOK := buildXorFilterFingerprints(reversed, seed)
		if orderedOK != reversedOK || orderedBlockLength != reversedBlockLength || !reflect.DeepEqual(orderedFingerprints, reversedFingerprints) {
			t.Fatalf("attempt %d reversed build = %v/%d/%v, want %v/%d/%v", attempt, reversedOK, reversedBlockLength, reversedFingerprints, orderedOK, orderedBlockLength, orderedFingerprints)
		}
		if orderedOK {
			return
		}
	}
	t.Fatal("ordered and reversed builds did not find a successful seed")
}

func TestXorFilterBuildSlotFitsQueueLinkInExistingPadding(t *testing.T) {
	if got := unsafe.Sizeof(xorFilterBuildSlot{}); got != 16 {
		t.Fatalf("sizeof(xorFilterBuildSlot) = %d, want 16", got)
	}
}

func TestXorFilterBuildAndContains(t *testing.T) {
	filter, err := newXorFilterData(4)
	if err != nil {
		t.Fatalf("newXorFilterData() error = %v", err)
	}
	added, err := filter.AddOne("alpha", "beta", "alpha", json.Number("3"))
	if err != nil {
		t.Fatalf("AddOne() error = %v", err)
	}
	if added != 3 {
		t.Fatalf("AddOne() = %d, want 3 unique staged values", added)
	}
	if _, queryable := filter.Contains("alpha"); queryable {
		t.Fatal("Contains() on pending XOR filter is queryable, want false")
	}
	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	for _, value := range []interface{}{"alpha", "beta", json.Number("3")} {
		if hit, queryable := filter.Contains(value); !queryable || !hit {
			t.Fatalf("Contains(%#v) = %v/%v, want hit", value, hit, queryable)
		}
	}
	missing := xorFilterMissingValue(t, filter)
	if hit, queryable := filter.Contains(missing); !queryable || hit {
		t.Fatalf("Contains(%q) = %v/%v, want miss", missing, hit, queryable)
	}
	info := filter.Info()
	if !info.Built || info.Items != 3 || info.Staged != 0 || info.FingerprintBytes == 0 || info.EstimatedFalsePositiveRate == 0 {
		t.Fatalf("Info() = %#v, want compact built XOR filter", info)
	}
	if _, err := filter.Add("late"); err == nil || !strings.Contains(err.Error(), "already built") {
		t.Fatalf("Add() after build error = %v, want already built", err)
	}
}

func TestXorFilterSnapshotRoundTrip(t *testing.T) {
	pending, err := newXorFilterData(8)
	if err != nil {
		t.Fatalf("newXorFilterData() error = %v", err)
	}
	if _, err := pending.AddOne("alpha", "beta"); err != nil {
		t.Fatalf("AddOne() error = %v", err)
	}
	restoredPending, err := newXorFilterDataFromSnapshot(pending.Snapshot())
	if err != nil {
		t.Fatalf("newXorFilterDataFromSnapshot(pending) error = %v", err)
	}
	if info := restoredPending.Info(); info.Built || info.Staged != 2 {
		t.Fatalf("restored pending Info() = %#v, want 2 staged", info)
	}
	if err := restoredPending.Build(); err != nil {
		t.Fatalf("Build(restored pending) error = %v", err)
	}
	if hit, queryable := restoredPending.Contains("alpha"); !queryable || !hit {
		t.Fatalf("Contains(alpha) after pending restore = %v/%v, want hit", hit, queryable)
	}

	built := restoredPending.Snapshot()
	restoredBuilt, err := newXorFilterDataFromSnapshot(built)
	if err != nil {
		t.Fatalf("newXorFilterDataFromSnapshot(built) error = %v", err)
	}
	if hit, queryable := restoredBuilt.Contains("beta"); !queryable || !hit {
		t.Fatalf("Contains(beta) after built restore = %v/%v, want hit", hit, queryable)
	}
}

func TestXorFilterSnapshotValidationRejectsInvalidStagedKey(t *testing.T) {
	err := validateXorFilterSnapshot(xorFilterSnapshot{
		ExpectedItems: 4,
		Items:         1,
		Staged: []xorFilterStagedItem{{
			Key:   `"wrong"`,
			Value: "alpha",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "does not match value") {
		t.Fatalf("validateXorFilterSnapshot() error = %v, want key mismatch", err)
	}
}

func TestXorFilterSnapshotValidationRejectsInvalidBuiltShape(t *testing.T) {
	tests := map[string]xorFilterSnapshot{
		"nonempty block without items": {
			ExpectedItems: 4,
			Built:         true,
			BlockLength:   2,
			Fingerprints:  base64.StdEncoding.EncodeToString(make([]byte, 6)),
		},
		"block length mismatch": {
			ExpectedItems: 4,
			Built:         true,
			Items:         2,
			BlockLength:   xorFilterBlockLength(2) + 1,
			Fingerprints:  base64.StdEncoding.EncodeToString(make([]byte, int(xorFilterBlockLength(2)+1)*3)),
		},
	}
	for name, snapshot := range tests {
		if err := validateXorFilterSnapshot(snapshot); err == nil {
			t.Fatalf("validateXorFilterSnapshot(%s) error = nil, want invalid built shape error", name)
		}
	}
}

func TestHatTrieXorFilterOperations(t *testing.T) {
	ht := newTestTrie(t)
	if err := ht.UpsertXorFilter("seen", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	hval := ht.Get("seen")
	if !hval.IsXorFilter() {
		t.Fatalf("UpsertXorFilter stored type %+v, want XOR filter", hval)
	}
	idx := hval.Index
	added, err := ht.AddXorFilter("seen", "alpha", "beta", "alpha")
	if err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if added != 2 {
		t.Fatalf("AddXorFilter() = %d, want 2", added)
	}
	if hit, queryable := ht.HasXorFilter("seen", "alpha"); queryable || hit {
		t.Fatalf("HasXorFilter() before build = %v/%v, want not queryable", hit, queryable)
	}
	info, ok, err := ht.BuildXorFilter("seen")
	if err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %#v/%v/%v, want ok", info, ok, err)
	}
	if !info.Built || info.Items != 2 || info.FingerprintBytes == 0 {
		t.Fatalf("BuildXorFilter() info = %#v, want built compact filter", info)
	}
	if hit, queryable := ht.HasXorFilter("seen", "alpha"); !queryable || !hit {
		t.Fatalf("HasXorFilter(alpha) = %v/%v, want hit", hit, queryable)
	}
	if hit, queryable := ht.HasXorFilter("seen", xorFilterMissingValue(t, ht.xorFilters.array[idx])); !queryable || hit {
		t.Fatalf("HasXorFilter(missing) = %v/%v, want miss", hit, queryable)
	}
	if err := ht.UpsertXorFilter("seen", 4); err != nil {
		t.Fatalf("UpsertXorFilter(replace) error = %v", err)
	}
	if got := ht.Get("seen"); !got.IsXorFilter() || got.Index != idx {
		t.Fatalf("UpsertXorFilter replacement stored %+v, want same XOR filter slot %d", got, idx)
	}
	added, err = ht.AddXorFilter("auto", "value")
	if err != nil || added != 1 {
		t.Fatalf("AddXorFilter(auto) = %d/%v, want 1/nil", added, err)
	}
	if !ht.Get("auto").IsXorFilter() {
		t.Fatal("AddXorFilter on missing key did not create an XOR filter")
	}
}

func TestHatTrieXorFilterDeleteReleasesBackingIndex(t *testing.T) {
	ht := newTestTrie(t)
	if err := ht.UpsertXorFilter("seen", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	if _, err := ht.AddXorFilter("seen", "alpha", "beta"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := ht.BuildXorFilter("seen"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() ok/error = %v/%v, want ok", ok, err)
	}
	idx := ht.Get("seen").Index

	if !ht.Delete("seen") {
		t.Fatal("Delete(seen) = false, want true")
	}
	if got := ht.Get("seen"); !got.Empty() {
		t.Fatalf("Delete(seen) left value %+v", got)
	}
	if !xorFilterIndexReleased(ht, idx) {
		t.Fatalf("deleted XOR filter index %d was not released", idx)
	}

	if err := ht.UpsertXorFilter("again", 8); err != nil {
		t.Fatalf("UpsertXorFilter(again) error = %v", err)
	}
	if got := ht.Get("again"); !got.IsXorFilter() || got.Index != idx {
		t.Fatalf("reused XOR filter value = %+v, want released slot %d", got, idx)
	}
}

func TestHatTrieAddXorFilterChecked(t *testing.T) {
	ht := newTestTrie(t)
	if added, err := ht.AddXorFilterChecked("seen", "alpha", "beta", "alpha"); err != nil || added != 2 {
		t.Fatalf("AddXorFilterChecked(seen) = %d/%v, want 2/nil", added, err)
	}
	info, ok, err := ht.XorFilterInfoChecked("seen")
	if err != nil || !ok || info.Staged != 2 || info.Items != 2 {
		t.Fatalf("XorFilterInfoChecked(seen) = %#v/%v/%v, want two staged items", info, ok, err)
	}

	if added, err := ht.AddXorFilterChecked("seen", "gamma", func() {}); err == nil || added != 0 {
		t.Fatalf("AddXorFilterChecked(unsupported batch) = %d/%v, want 0/error", added, err)
	}
	info, ok, err = ht.XorFilterInfoChecked("seen")
	if err != nil || !ok || info.Staged != 2 || info.Items != 2 {
		t.Fatalf("XorFilterInfoChecked(after rejected batch) = %#v/%v/%v, want unchanged two staged items", info, ok, err)
	}

	if added, err := ht.AddXorFilterChecked("missing", func() {}); err == nil || added != 0 {
		t.Fatalf("AddXorFilterChecked(missing unsupported) = %d/%v, want 0/error", added, err)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected AddXorFilterChecked created value %+v", got)
	}
}

func TestHatTrieXorFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	added, err := ht.AddXorFilter("seen", "alpha")
	if err != nil || added != 1 {
		t.Fatalf("AddXorFilter(alpha) = %d/%v, want 1/nil", added, err)
	}

	if added, err := ht.AddXorFilter("seen", "beta", func() {}); err == nil {
		t.Fatalf("AddXorFilter(unsupported batch) = %d/nil, want error", added)
	}
	info, ok := ht.XorFilterInfo("seen")
	if !ok || info.Staged != 1 || info.Items != 1 {
		t.Fatalf("XorFilterInfo(after rejected add) = %#v/%v, want one staged item", info, ok)
	}

	if added, err := ht.AddXorFilter("missing", func() {}); err == nil {
		t.Fatalf("AddXorFilter(missing unsupported) = %d/nil, want error", added)
	}
	if got := ht.Get("missing"); !got.Empty() {
		t.Fatalf("rejected missing-key XOR filter left value %+v", got)
	}
	ht.UpsertString("string", "keep")
	if added, err := ht.AddXorFilter("string", func() {}); err == nil {
		t.Fatalf("AddXorFilter(replacement unsupported) = %d/nil, want error", added)
	}
	if got := ht.GetString("string"); got != "keep" {
		t.Fatalf("rejected replacement changed string to %q, want keep", got)
	}

	if _, _, err := ht.HasXorFilterChecked("seen", func() {}); err == nil {
		t.Fatal("HasXorFilterChecked(unsupported) error = nil, want error")
	}
	if hit, queryable := ht.HasXorFilter("seen", func() {}); hit || queryable {
		t.Fatalf("HasXorFilter legacy unsupported = %v/%v, want false/false", hit, queryable)
	}
	if _, _, err := ht.BuildXorFilter("seen"); err != nil {
		t.Fatalf("BuildXorFilter() error = %v", err)
	}
	if hit, queryable, err := ht.HasXorFilterChecked("seen", func() {}); err == nil {
		t.Fatalf("HasXorFilterChecked(unsupported built) = %v/%v/nil, want error", hit, queryable)
	}
}

func TestHatTrieXorFilterRejectsInvalidExpectedItems(t *testing.T) {
	for _, expectedItems := range []uint64{0, maxXorFilterItems + 1} {
		if err := htUpsertXorFilterForTest(expectedItems); err == nil {
			t.Fatalf("UpsertXorFilter(%d) error = nil, want error", expectedItems)
		}
	}
}

func TestXorFilterLargeEmptyExpectedItemsAllocatesLazily(t *testing.T) {
	filter, err := newXorFilterData(maxXorFilterItems)
	if err != nil {
		t.Fatalf("newXorFilterData(max) error = %v", err)
	}
	if filter.staged != nil {
		t.Fatal("empty max-sized XOR filter allocated staged map, want lazy nil map")
	}
	if info := filter.Info(); info.ExpectedItems != maxXorFilterItems || info.Staged != 0 || info.Items != 0 || info.Built {
		t.Fatalf("Info(empty max) = %#v, want pending filter with logical expected item count", info)
	}

	restored, err := newXorFilterDataFromSnapshot(xorFilterSnapshot{ExpectedItems: maxXorFilterItems})
	if err != nil {
		t.Fatalf("newXorFilterDataFromSnapshot(empty max) error = %v", err)
	}
	if restored.staged != nil {
		t.Fatal("restored empty max-sized XOR filter allocated staged map, want lazy nil map")
	}

	if added, err := filter.AddOne("alpha"); err != nil || added != 1 {
		t.Fatalf("AddOne(first) = %d/%v, want one staged item", added, err)
	}
	if len(filter.staged) != 1 {
		t.Fatalf("staged map after first add has %d items, want 1", len(filter.staged))
	}

	ht := newTestTrie(t)
	if err := ht.UpsertXorFilter("seen", maxXorFilterItems); err != nil {
		t.Fatalf("UpsertXorFilter(max) error = %v", err)
	}
	hval := ht.Get("seen")
	if ht.xorFilters.array[hval.Index].staged != nil {
		t.Fatal("empty trie XOR filter allocated staged map, want lazy nil map")
	}
	if added, err := ht.AddXorFilterChecked("seen", "alpha"); err != nil || added != 1 {
		t.Fatalf("AddXorFilterChecked(first) = %d/%v, want one staged item", added, err)
	}
	if len(ht.xorFilters.array[hval.Index].staged) != 1 {
		t.Fatalf("trie XOR filter staged map after first add has %d items, want 1", len(ht.xorFilters.array[hval.Index].staged))
	}
}

func htUpsertXorFilterForTest(expectedItems uint64) error {
	ht := CreateHatTrie()
	defer ht.Destroy()
	return ht.UpsertXorFilter("bad", expectedItems)
}

func BenchmarkXorFilterLifecyclePhases64(b *testing.B) {
	values := make([]string, 64)
	keys := make([]string, 64)
	for item := range values {
		values[item] = fmt.Sprintf("value-%d", item)
		keys[item] = xorFilterJSONStringKey(values[item])
	}
	sort.Strings(keys)

	b.Run("PlainStringStage", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			filter := xorFilterData{}
			for _, value := range values {
				if _, err := filter.addJSONString(value); err != nil {
					b.Fatal(err)
				}
			}
			benchmarkXorFilterAttemptSink = len(filter.staged)
		}
	})

	b.Run("FingerprintBuild", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
				fingerprints, _, ok := buildXorFilterFingerprints(keys, xorFilterSeed(len(keys), attempt))
				if !ok {
					continue
				}
				benchmarkXorFilterFingerprintsSink = fingerprints
				benchmarkXorFilterAttemptSink = attempt
				break
			}
		}
	})

	filter := xorFilterData{}
	for _, value := range values {
		if _, err := filter.addJSONString(value); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("PendingSnapshot", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkXorFilterSnapshotSink = filter.Snapshot()
		}
	})
}

func BenchmarkXorFilterBuildKeyOrderAlternating(b *testing.B) {
	for _, items := range []int{64, 4096, 65536} {
		staged := make(map[string]interface{}, items)
		for idx := 0; idx < items; idx++ {
			key := xorFilterJSONStringKey(fmt.Sprintf("order-%d", idx))
			staged[key] = idx
		}
		b.Run(strconv.Itoa(items), func(b *testing.B) {
			var sortedDuration, unsortedDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				unsortedFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					fingerprints, attempt := benchmarkBuildXorFilterFromStaged(staged, unsortedFirst == (pass == 0))
					if fingerprints == nil {
						b.Fatal("fingerprint build failed")
					}
					benchmarkXorFilterFingerprintsSink = fingerprints
					benchmarkXorFilterAttemptSink = attempt
					if unsortedFirst == (pass == 0) {
						unsortedDuration += time.Since(started)
					} else {
						sortedDuration += time.Since(started)
					}
				}
			}
			b.ReportMetric(float64(sortedDuration.Nanoseconds())/float64(b.N), "sorted-ns/build")
			b.ReportMetric(float64(unsortedDuration.Nanoseconds())/float64(b.N), "unsorted-ns/build")
		})
	}
}

func BenchmarkXorFilterBuildKeyOrderAllocations(b *testing.B) {
	for _, items := range []int{64, 4096, 65536} {
		staged := make(map[string]interface{}, items)
		for idx := 0; idx < items; idx++ {
			key := xorFilterJSONStringKey(fmt.Sprintf("order-%d", idx))
			staged[key] = idx
		}
		for _, benchmark := range []struct {
			name     string
			unsorted bool
		}{
			{name: "Sorted"},
			{name: "Unsorted", unsorted: true},
		} {
			b.Run(strconv.Itoa(items)+"/"+benchmark.name, func(b *testing.B) {
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					fingerprints, attempt := benchmarkBuildXorFilterFromStaged(staged, benchmark.unsorted)
					if fingerprints == nil {
						b.Fatal("fingerprint build failed")
					}
					benchmarkXorFilterFingerprintsSink = fingerprints
					benchmarkXorFilterAttemptSink = attempt
				}
			})
		}
	}
}

func benchmarkBuildXorFilterFromStaged(staged map[string]interface{}, unsorted bool) ([]uint8, int) {
	keys := make([]string, 0, len(staged))
	for key := range staged {
		keys = append(keys, key)
	}
	if !unsorted {
		sort.Strings(keys)
	}
	for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
		fingerprints, _, ok := buildXorFilterFingerprints(keys, xorFilterSeed(len(keys), attempt))
		if ok {
			return fingerprints, attempt
		}
	}
	return nil, -1
}

func BenchmarkXorFilterFingerprintBuild(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		items int
		salt  int
	}{
		{name: "Retry3", items: 3, salt: 45},
		{name: "Items64", items: 64},
		{name: "Items4096", items: 4096},
		{name: "Items65536", items: 65536},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			keys := make([]string, fixture.items)
			for item := range keys {
				keys[item] = xorFilterJSONStringKey(fmt.Sprintf("retry-%d-value-%d", fixture.salt, item))
			}
			sort.Strings(keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
					fingerprints, _, ok := buildXorFilterFingerprints(keys, xorFilterSeed(len(keys), attempt))
					if !ok {
						continue
					}
					benchmarkXorFilterFingerprintsSink = fingerprints
					benchmarkXorFilterAttemptSink = attempt
					break
				}
			}
		})
	}
}

func BenchmarkXorFilterQueueLayout(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		items int
		salt  int
	}{
		{name: "Retry3", items: 3, salt: 45},
		{name: "Items64", items: 64},
		{name: "Items4096", items: 4096},
		{name: "Items65536", items: 65536},
	} {
		keys := make([]string, fixture.items)
		for item := range keys {
			keys[item] = xorFilterJSONStringKey(fmt.Sprintf("retry-%d-value-%d", fixture.salt, item))
		}
		sort.Strings(keys)
		for _, layout := range []struct {
			name  string
			build func([]string, uint64) ([]uint8, uint32, bool)
		}{
			{name: "SliceQueueControl", build: buildXorFilterFingerprintsSliceQueueControl},
			{name: "LinkedQueue", build: buildXorFilterFingerprints},
		} {
			b.Run(fixture.name+"/"+layout.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
						fingerprints, _, ok := layout.build(keys, xorFilterSeed(len(keys), attempt))
						if !ok {
							continue
						}
						benchmarkXorFilterFingerprintsSink = fingerprints
						benchmarkXorFilterAttemptSink = attempt
						break
					}
				}
			})
		}
	}
}

func buildXorFilterFingerprintsSliceQueueControl(keys []string, seed uint64) ([]uint8, uint32, bool) {
	blockLength := xorFilterBlockLength(uint64(len(keys)))
	if blockLength == 0 {
		return nil, 0, true
	}
	size := int(blockLength) * 3
	slots := make([]xorFilterBuildSlot, size)
	for _, key := range keys {
		hash := xorFilterHashString(key, seed)
		for _, index := range xorFilterIndexes(hash, blockLength) {
			slots[index].count++
			slots[index].xor ^= hash
		}
	}

	queue := make([]uint32, 0, size)
	for index, slot := range slots {
		if slot.count == 1 {
			queue = append(queue, uint32(index))
		}
	}

	order := make([]xorFilterPeel, 0, len(keys))
	for head := 0; head < len(queue); head++ {
		index := queue[head]
		slot := slots[index]
		if slot.count != 1 {
			continue
		}
		hash := slot.xor
		order = append(order, xorFilterPeel{hash: hash, index: index})
		for _, other := range xorFilterIndexes(hash, blockLength) {
			if slots[other].count == 0 {
				continue
			}
			slots[other].count--
			slots[other].xor ^= hash
			if slots[other].count == 1 {
				queue = append(queue, other)
			}
		}
	}
	if len(order) != len(keys) {
		return nil, blockLength, false
	}

	fingerprints := make([]uint8, size)
	for pos := len(order) - 1; pos >= 0; pos-- {
		item := order[pos]
		fingerprint := xorFilterFingerprint(item.hash)
		for _, index := range xorFilterIndexes(item.hash, blockLength) {
			if index == item.index {
				continue
			}
			fingerprint ^= fingerprints[index]
		}
		fingerprints[item.index] = fingerprint
	}
	return fingerprints, blockLength, true
}

func xorFilterMissingValue(t *testing.T, filter xorFilterData) string {
	t.Helper()
	for idx := 0; idx < 10000; idx++ {
		candidate := fmt.Sprintf("missing-%d", idx)
		hit, queryable := filter.Contains(candidate)
		if queryable && !hit {
			return candidate
		}
	}
	t.Fatal("could not find deterministic XOR filter miss")
	return ""
}
