package hatDataStructure_test

import (
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestIndexStatsTracksCardinalityPostingAndHotKeys(t *testing.T) {
	stats, err := hatDataStructure.NewIndexStats(hatDataStructure.IndexStatsOptions{
		HotKeyCapacity: 2,
		HLLPrecision:   10,
	})
	if err != nil {
		t.Fatalf("NewIndexStats() error = %v", err)
	}
	stats.ObserveKeyHash(1)
	stats.ObserveKeyHash(2)
	stats.ObserveKeyHash(1)
	stats.ObserveLookup(42, 10)
	stats.ObserveLookup(42, 14)
	stats.ObserveLookup(42, 12)
	stats.ObserveLookup(7, 4)

	snapshot := stats.Snapshot()
	if snapshot.CardinalityObservations != 3 {
		t.Fatalf("CardinalityObservations = %d, want 3", snapshot.CardinalityObservations)
	}
	if snapshot.LookupObservations != 4 {
		t.Fatalf("LookupObservations = %d, want 4", snapshot.LookupObservations)
	}
	if snapshot.PostingLengthSamples != 4 {
		t.Fatalf("PostingLengthSamples = %d, want 4", snapshot.PostingLengthSamples)
	}
	if snapshot.TotalPostingLength != 40 {
		t.Fatalf("TotalPostingLength = %d, want 40", snapshot.TotalPostingLength)
	}
	if snapshot.MaxPostingLength != 14 {
		t.Fatalf("MaxPostingLength = %d, want 14", snapshot.MaxPostingLength)
	}
	if snapshot.EstimatedDistinctKeys < 3 {
		t.Fatalf("EstimatedDistinctKeys = %d, want at least 3", snapshot.EstimatedDistinctKeys)
	}
	if len(snapshot.HotKeys) != 2 {
		t.Fatalf("len(HotKeys) = %d, want 2", len(snapshot.HotKeys))
	}
	if got := snapshot.HotKeys[0]; got.KeyHash != 42 || got.Observations != 3 || got.MaxPostingLength != 14 {
		t.Fatalf("top hot key = %+v, want hash 42/count 3/max 14", got)
	}
	if got := snapshot.HotKeys[1]; got.KeyHash != 7 || got.Observations != 1 || got.MaxPostingLength != 4 {
		t.Fatalf("second hot key = %+v, want hash 7/count 1/max 4", got)
	}
}

func TestIndexStatsSpaceSavingKeepsBoundedHotKeys(t *testing.T) {
	stats, err := hatDataStructure.NewIndexStats(hatDataStructure.IndexStatsOptions{HotKeyCapacity: 2})
	if err != nil {
		t.Fatalf("NewIndexStats() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		stats.ObserveLookup(1, 1)
	}
	stats.ObserveLookup(2, 1)
	for i := 0; i < 3; i++ {
		stats.ObserveLookup(3, 1)
	}

	hotKeys := stats.Snapshot().HotKeys
	if len(hotKeys) != 2 {
		t.Fatalf("len(HotKeys) = %d, want 2", len(hotKeys))
	}
	if hotKeys[0].KeyHash != 1 || hotKeys[1].KeyHash != 3 {
		t.Fatalf("hot key hashes = %d, %d; want 1, 3", hotKeys[0].KeyHash, hotKeys[1].KeyHash)
	}
	if hotKeys[1].Error == 0 {
		t.Fatalf("evicted-key error = 0, want a positive estimate")
	}
}

func TestIndexStatsSnapshotOwnsHotKeys(t *testing.T) {
	stats := hatDataStructure.NewDefaultIndexStats()
	stats.ObserveLookup(42, 1)
	snapshot := stats.Snapshot()
	snapshot.HotKeys[0].KeyHash = 99
	if got := stats.Snapshot().HotKeys[0].KeyHash; got != 42 {
		t.Fatalf("stored hot key hash = %d, want 42", got)
	}
}

func TestIndexStatsRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		options hatDataStructure.IndexStatsOptions
	}{
		{name: "negative capacity", options: hatDataStructure.IndexStatsOptions{HotKeyCapacity: -1}},
		{name: "capacity too large", options: hatDataStructure.IndexStatsOptions{HotKeyCapacity: hatDataStructure.MaxIndexStatsHotKeyCapacity + 1}},
		{name: "precision too large", options: hatDataStructure.IndexStatsOptions{HLLPrecision: 255}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatDataStructure.NewIndexStats(test.options); err == nil {
				t.Fatal("NewIndexStats() succeeded, want error")
			}
		})
	}
	if _, err := hatDataStructure.NewIndexStats(hatDataStructure.IndexStatsOptions{HotKeyCapacity: -1}); !errors.Is(err, hatDataStructure.ErrIndexStatsHotKeyCapacityInvalid) {
		t.Fatalf("capacity error = %v, want ErrIndexStatsHotKeyCapacityInvalid", err)
	}
}

func TestIndexStatsConcurrentObservation(t *testing.T) {
	stats := hatDataStructure.NewDefaultIndexStats()
	const goroutines = 16
	const observations = 250
	var wait sync.WaitGroup
	wait.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wait.Done()
			for j := 0; j < observations; j++ {
				stats.ObserveLookup(42, 8)
			}
		}()
	}
	wait.Wait()

	snapshot := stats.Snapshot()
	if snapshot.LookupObservations != goroutines*observations {
		t.Fatalf("LookupObservations = %d, want %d", snapshot.LookupObservations, goroutines*observations)
	}
	if snapshot.TotalPostingLength != goroutines*observations*8 {
		t.Fatalf("TotalPostingLength = %d, want %d", snapshot.TotalPostingLength, goroutines*observations*8)
	}
	if len(snapshot.HotKeys) != 1 || snapshot.HotKeys[0].Observations != goroutines*observations {
		t.Fatalf("hot key snapshot = %+v, want one exact counter", snapshot.HotKeys)
	}
}

func BenchmarkIndexStatsObserveLookup(b *testing.B) {
	stats := hatDataStructure.NewDefaultIndexStats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.ObserveLookup(uint64(i&255), uint64(i&63))
	}
}

func BenchmarkIndexStatsObserveKeyHash(b *testing.B) {
	stats := hatDataStructure.NewDefaultIndexStats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.ObserveKeyHash(uint64(i & 255))
	}
}
