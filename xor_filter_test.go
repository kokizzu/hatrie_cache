package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

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
