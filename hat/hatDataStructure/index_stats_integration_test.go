package hatDataStructure

import (
	"errors"
	"hash/fnv"
	"sync"
	"testing"
)

type indexStatsTestValue struct {
	Key   string
	Value string
}

func indexStatsTestHash(key string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum64()
}

func TestHashIndexStatsAttachment(t *testing.T) {
	index, err := NewHashIndex(func(value indexStatsTestValue) string { return value.Key }, HashIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := NewIndexStats(IndexStatsOptions{HotKeyCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.AttachStats(stats, indexStatsTestHash); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, indexStatsTestValue{Key: "a", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, indexStatsTestValue{Key: "a", Value: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(3, indexStatsTestValue{Key: "b", Value: "three"}); err != nil {
		t.Fatal(err)
	}
	if got := len(index.Lookup("a")); got != 2 {
		t.Fatalf("Lookup(a) length = %d, want 2", got)
	}
	if got := len(index.Lookup("missing")); got != 0 {
		t.Fatalf("Lookup(missing) length = %d, want 0", got)
	}
	if got := len(index.LookupIDs("a")); got != 2 {
		t.Fatalf("LookupIDs(a) length = %d, want 2", got)
	}
	if !index.Contains("b") {
		t.Fatal("Contains(b) = false, want true")
	}
	if _, ok := index.LookupOne("a"); !ok {
		t.Fatal("LookupOne(a) = miss, want hit")
	}

	snapshot := index.Stats()
	if snapshot.LookupObservations != 5 {
		t.Fatalf("lookup observations = %d, want 5", snapshot.LookupObservations)
	}
	if snapshot.TotalPostingLength != 7 {
		t.Fatalf("total posting length = %d, want 7", snapshot.TotalPostingLength)
	}
	if snapshot.CardinalityObservations != 3 {
		t.Fatalf("cardinality observations = %d, want 3", snapshot.CardinalityObservations)
	}
	if snapshot.EstimatedDistinctKeys < 2 {
		t.Fatalf("estimated distinct keys = %d, want at least 2", snapshot.EstimatedDistinctKeys)
	}
	if len(snapshot.HotKeys) == 0 || snapshot.HotKeys[0].KeyHash != indexStatsTestHash("a") || snapshot.HotKeys[0].Observations != 3 {
		t.Fatalf("hot keys = %#v, want key a with 3 observations", snapshot.HotKeys)
	}

	index.DetachStats()
	if snapshot := index.Stats(); snapshot.LookupObservations != 0 {
		t.Fatalf("detached lookup observations = %d, want 0", snapshot.LookupObservations)
	}
}

func TestFunctionalIndexStatsAttachment(t *testing.T) {
	index, err := NewFunctionalIndex(func(value indexStatsTestValue) string { return value.Key }, 0)
	if err != nil {
		t.Fatal(err)
	}
	stats := NewDefaultIndexStats()
	if err := index.AttachStats(stats, indexStatsTestHash); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, indexStatsTestValue{Key: "x", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if got := len(index.LookupIDs("x")); got != 1 {
		t.Fatalf("LookupIDs(x) length = %d, want 1", got)
	}
	if got := len(index.Lookup("unknown")); got != 0 {
		t.Fatalf("Lookup(unknown) length = %d, want 0", got)
	}
	if got := index.Stats().LookupObservations; got != 2 {
		t.Fatalf("lookup observations = %d, want 2", got)
	}
}

func TestOrderedIndexStatsManualObservation(t *testing.T) {
	index, err := NewOrderedIndex(
		func(value indexStatsTestValue) string { return value.Key },
		func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, indexStatsTestValue{Key: "a", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(2, indexStatsTestValue{Key: "b", Value: "two"}); err != nil {
		t.Fatal(err)
	}
	stats := NewDefaultIndexStats()
	stats.ObserveKeyHash(indexStatsTestHash("a"))
	stats.ObserveKeyHash(indexStatsTestHash("b"))
	iterator, ok := index.Seek("b")
	if !ok {
		t.Fatal("Seek(b) = miss, want hit")
	}
	iterator.Close()
	stats.ObserveLookup(indexStatsTestHash("b"), 1)
	if _, ok := index.Seek("missing"); ok {
		t.Fatal("Seek(missing) = hit, want miss")
	}
	stats.ObserveLookup(indexStatsTestHash("missing"), 0)
	rangeIterator, ok := index.Range("a", "b")
	if !ok {
		t.Fatal("Range(a,b) = miss, want hit")
	}
	rangeIterator.Close()
	stats.ObserveLookup(indexStatsTestHash("a"), 2)
	snapshot := stats.Snapshot()
	if snapshot.LookupObservations != 3 {
		t.Fatalf("lookup observations = %d, want 3", snapshot.LookupObservations)
	}
	if snapshot.TotalPostingLength != 3 {
		t.Fatalf("total posting length = %d, want 3", snapshot.TotalPostingLength)
	}
	if snapshot.CardinalityObservations != 2 {
		t.Fatalf("cardinality observations = %d, want 2", snapshot.CardinalityObservations)
	}
}

func TestIndexStatsAttachmentRequiresCollectorAndHasher(t *testing.T) {
	index, err := NewHashIndex(func(value indexStatsTestValue) string { return value.Key }, HashIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.AttachStats(nil, indexStatsTestHash); !errors.Is(err, ErrIndexStatsCollectorRequired) {
		t.Fatalf("nil collector error = %v, want %v", err, ErrIndexStatsCollectorRequired)
	}
	if err := index.AttachStats(NewDefaultIndexStats(), nil); !errors.Is(err, ErrIndexStatsHasherRequired) {
		t.Fatalf("nil hasher error = %v, want %v", err, ErrIndexStatsHasherRequired)
	}
}

func TestHashIndexStatsAttachmentConcurrentLifecycle(t *testing.T) {
	index, err := NewHashIndex(func(value indexStatsTestValue) string { return value.Key }, HashIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(1, indexStatsTestValue{Key: "a", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	stats := NewDefaultIndexStats()
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				_, _ = index.LookupOne("a")
				_ = index.Lookup("missing")
			}
		}()
	}
	for iteration := 0; iteration < 50; iteration++ {
		if err := index.AttachStats(stats, indexStatsTestHash); err != nil {
			t.Fatal(err)
		}
		index.DetachStats()
	}
	waitGroup.Wait()
}
