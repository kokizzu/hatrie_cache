package hatriecache

import (
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

func TestHatTrieXorFilterRejectsInvalidExpectedItems(t *testing.T) {
	for _, expectedItems := range []uint64{0, maxXorFilterItems + 1} {
		if err := htUpsertXorFilterForTest(expectedItems); err == nil {
			t.Fatalf("UpsertXorFilter(%d) error = nil, want error", expectedItems)
		}
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
