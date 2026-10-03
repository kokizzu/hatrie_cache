package hatDataStructure

import (
	"errors"
	"testing"
)

func round77AdmissionHash(key uint64) uint64 {
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	return key ^ (key >> 31)
}

func TestFrequencyAdmissionConstructionValidation(t *testing.T) {
	tests := []struct {
		name    string
		options FrequencyAdmissionCacheOptions[uint64]
		wantErr error
	}{
		{name: "zero capacity", options: FrequencyAdmissionCacheOptions[uint64]{Hash: round77AdmissionHash}, wantErr: ErrFrequencyAdmissionCacheCapacityInvalid},
		{name: "negative capacity", options: FrequencyAdmissionCacheOptions[uint64]{Capacity: -1, Hash: round77AdmissionHash}, wantErr: ErrFrequencyAdmissionCacheCapacityInvalid},
		{name: "missing hash", options: FrequencyAdmissionCacheOptions[uint64]{Capacity: 2}, wantErr: ErrFrequencyAdmissionCacheHashRequired},
		{name: "small counter table", options: FrequencyAdmissionCacheOptions[uint64]{Capacity: 2, Hash: round77AdmissionHash, CounterCount: 3}, wantErr: ErrFrequencyAdmissionCacheCounterCountInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewFrequencyAdmissionCache[uint64, string](test.options); !errors.Is(err, test.wantErr) {
				t.Fatalf("NewFrequencyAdmissionCache() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestFrequencyAdmissionCachesUpdatesDeletesAndClears(t *testing.T) {
	cache, err := NewFrequencyAdmissionCache[uint64, string](FrequencyAdmissionCacheOptions[uint64]{
		Capacity: 2,
		Hash:     round77AdmissionHash,
	})
	if err != nil {
		t.Fatalf("NewFrequencyAdmissionCache() error = %v", err)
	}
	if cache.Len() != 0 {
		t.Fatalf("empty Len() = %d, want 0", cache.Len())
	}
	if value, ok := cache.Get(1); ok || value != "" {
		t.Fatalf("empty Get() = %q, %v", value, ok)
	}
	if !cache.Set(1, "one") || !cache.Set(2, "two") {
		t.Fatal("initial Set() rejected an entry")
	}
	if !cache.Set(1, "updated") {
		t.Fatal("updating an existing entry was rejected")
	}
	if value, ok := cache.Get(1); !ok || value != "updated" {
		t.Fatalf("updated Get() = %q, %v", value, ok)
	}
	if !cache.Delete(2) || cache.Delete(2) {
		t.Fatal("Delete() did not report exactly one removal")
	}
	if cache.Len() != 1 {
		t.Fatalf("Len() after delete = %d, want 1", cache.Len())
	}
	cache.Clear()
	if cache.Len() != 0 {
		t.Fatalf("Len() after Clear() = %d, want 0", cache.Len())
	}
	if _, ok := cache.Get(1); ok {
		t.Fatal("cleared key remained readable")
	}
	stats := cache.Stats()
	if stats.Capacity != 2 || stats.CounterBytes == 0 || stats.Misses == 0 {
		t.Fatalf("unexpected stats after clear: %#v", stats)
	}
}

func TestFrequencyAdmissionRejectsColdScanAndRetainsHotEntries(t *testing.T) {
	cache, err := NewFrequencyAdmissionCache[uint64, uint64](FrequencyAdmissionCacheOptions[uint64]{
		Capacity:     2,
		Hash:         round77AdmissionHash,
		CounterCount: 64,
		SampleWindow: 10_000,
	})
	if err != nil {
		t.Fatalf("NewFrequencyAdmissionCache() error = %v", err)
	}
	if !cache.Set(1, 1) || !cache.Set(2, 2) {
		t.Fatal("initial entries were not admitted")
	}
	for index := 0; index < 16; index++ {
		if _, ok := cache.Get(1); !ok {
			t.Fatal("hot entry disappeared before scan")
		}
	}
	if cache.Set(3, 3) {
		t.Fatal("cold scan entry displaced a more frequently used entry")
	}
	if _, ok := cache.Get(3); ok {
		t.Fatal("rejected scan entry became resident")
	}
	for index := 0; index < 4; index++ {
		_, _ = cache.Get(4)
	}
	if !cache.Set(4, 4) {
		t.Fatal("warmed candidate was not admitted")
	}
	if _, ok := cache.Get(1); !ok {
		t.Fatal("hot entry was evicted by a warmed candidate")
	}
	if _, ok := cache.Get(2); ok {
		t.Fatal("least-recently-used cold entry was not evicted")
	}
	stats := cache.Stats()
	if stats.Rejected == 0 || stats.Evictions == 0 || stats.Admitted < 3 {
		t.Fatalf("admission stats did not record policy decisions: %#v", stats)
	}
}

func TestFrequencyAdmissionCounterAgesWithoutChangingValues(t *testing.T) {
	cache, err := NewFrequencyAdmissionCache[uint64, string](FrequencyAdmissionCacheOptions[uint64]{
		Capacity:     2,
		Hash:         round77AdmissionHash,
		CounterCount: 64,
		SampleWindow: 4,
	})
	if err != nil {
		t.Fatalf("NewFrequencyAdmissionCache() error = %v", err)
	}
	if !cache.Set(7, "seven") {
		t.Fatal("Set() rejected initial value")
	}
	for index := 0; index < 12; index++ {
		if _, ok := cache.Get(7); !ok {
			t.Fatal("value was lost while counters aged")
		}
		_, _ = cache.Get(uint64(100 + index))
	}
	if value, ok := cache.Get(7); !ok || value != "seven" {
		t.Fatalf("aged value = %q, %v", value, ok)
	}
	if cache.Stats().Aged == 0 {
		t.Fatal("counter aging was not observed")
	}
}
