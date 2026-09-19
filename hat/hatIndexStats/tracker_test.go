package hatIndexStats

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestTrackerSnapshotReportsIndexStats(t *testing.T) {
	tracker, err := New(Config{TopK: 2, SampleEvery: 1, MaxKeyBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	tracker.Observe([]byte("hot"), 3, true)
	tracker.Observe([]byte("cold"), 1, false)
	tracker.Observe([]byte("hot"), 5, true)
	tracker.Observe([]byte("other"), 2, true)
	tracker.Observe([]byte("hot"), 4, true)

	snapshot := tracker.Snapshot()
	if snapshot.Operations != 5 || snapshot.Hits != 4 || snapshot.Misses != 1 {
		t.Fatalf("operation counters = %+v", snapshot)
	}
	if snapshot.SampledOperations != 5 || snapshot.SampledPostingTotal != 15 || snapshot.MaxPostingLength != 5 {
		t.Fatalf("sample counters = %+v", snapshot)
	}
	if snapshot.ApproxDistinct < 2 || snapshot.ApproxDistinct > 4 {
		t.Fatalf("ApproxDistinct = %d, want near 3", snapshot.ApproxDistinct)
	}
	if len(snapshot.TopKeys) != 2 || string(snapshot.TopKeys[0].Key) != "hot" || snapshot.TopKeys[0].EstimatedCount != 3 {
		t.Fatalf("TopKeys = %+v", snapshot.TopKeys)
	}
}

func TestTrackerSamplingBoundsHotKeyWork(t *testing.T) {
	tracker, err := New(Config{TopK: 1, SampleEvery: 4, MaxKeyBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		tracker.Observe([]byte("long-key"), uint64(i), true)
	}
	snapshot := tracker.Snapshot()
	if snapshot.Operations != 9 || snapshot.SampledOperations != 2 {
		t.Fatalf("sampling counters = %+v", snapshot)
	}
	if len(snapshot.TopKeys) != 1 || snapshot.TopKeys[0].HasKey {
		t.Fatalf("long key should be represented by hash only: %+v", snapshot.TopKeys)
	}
}

func TestTrackerReplacesTheLeastFrequentHotKey(t *testing.T) {
	tracker, err := New(Config{TopK: 1, SampleEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	tracker.Observe([]byte("first"), 1, true)
	tracker.Observe([]byte("second"), 1, true)
	tracker.Observe([]byte("second"), 1, true)
	snapshot := tracker.Snapshot()
	if len(snapshot.TopKeys) != 1 || string(snapshot.TopKeys[0].Key) != "second" || snapshot.TopKeys[0].EstimatedCount != 3 {
		t.Fatalf("replacement snapshot = %+v", snapshot)
	}
}

func TestTrackerSnapshotIsolatedAndReset(t *testing.T) {
	tracker, err := New(Config{SampleEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	tracker.Observe([]byte("key"), 1, true)
	snapshot := tracker.Snapshot()
	snapshot.TopKeys[0].Key[0] = 'X'
	if bytes.Equal(snapshot.TopKeys[0].Key, tracker.Snapshot().TopKeys[0].Key) {
		t.Fatal("Snapshot returned tracker-owned key bytes")
	}
	tracker.Reset()
	if got := tracker.Snapshot(); got.Operations != 0 || len(got.TopKeys) != 0 {
		t.Fatalf("snapshot after Reset = %+v", got)
	}
}

func TestTrackerConcurrentUse(t *testing.T) {
	tracker, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 1000; j++ {
				tracker.Observe([]byte("key"), 1, j%2 == 0)
			}
		}()
	}
	group.Wait()
	if got := tracker.Snapshot().Operations; got != 8000 {
		t.Fatalf("Operations = %d, want 8000", got)
	}
}

func TestTrackerRejectsInvalidConfig(t *testing.T) {
	if _, err := New(Config{TopK: maxTopK + 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("large TopK error = %v, want %v", err, ErrInvalidConfig)
	}
	if _, err := New(Config{SampleEvery: 0, MaxKeyBytes: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative MaxKeyBytes error = %v, want %v", err, ErrInvalidConfig)
	}
}
