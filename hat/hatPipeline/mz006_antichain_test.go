//go:build !mz006baseline

package hatPipeline

import (
	"errors"
	"sync"
	"testing"
)

type mz006Timestamp struct {
	epoch  uint64
	offset uint64
}

func mz006TimestampLessEqual(left, right mz006Timestamp) bool {
	return left.epoch <= right.epoch && left.offset <= right.offset
}

func TestMZ006AntichainRemovesDominatedTimestamps(t *testing.T) {
	antichain, err := NewAntichain(mz006TimestampLessEqual)
	if err != nil {
		t.Fatal(err)
	}

	for _, timestamp := range []mz006Timestamp{{5, 1}, {4, 2}} {
		changed, err := antichain.Insert(timestamp)
		if err != nil || !changed {
			t.Fatalf("Insert(%#v) = %t, %v; want changed", timestamp, changed, err)
		}
	}
	if changed, err := antichain.Insert(mz006Timestamp{6, 3}); err != nil || changed {
		t.Fatalf("dominated Insert() = %t, %v; want unchanged", changed, err)
	}
	if changed, err := antichain.Insert(mz006Timestamp{4, 1}); err != nil || !changed {
		t.Fatalf("dominating Insert() = %t, %v; want changed", changed, err)
	}

	got, err := antichain.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := []mz006Timestamp{{4, 1}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
	covered, err := antichain.Covers(mz006Timestamp{4, 9})
	if err != nil || !covered {
		t.Fatalf("Covers(4,9) = %t, %v; want true", covered, err)
	}
	covered, err = antichain.Covers(mz006Timestamp{3, 9})
	if err != nil || covered {
		t.Fatalf("Covers(3,9) = %t, %v; want false", covered, err)
	}
}

func TestMZ006AntichainKeepsIncomparableFrontier(t *testing.T) {
	antichain, err := NewAntichain(mz006TimestampLessEqual)
	if err != nil {
		t.Fatal(err)
	}
	for _, timestamp := range []mz006Timestamp{{1, 5}, {5, 1}} {
		if changed, err := antichain.Insert(timestamp); err != nil || !changed {
			t.Fatalf("Insert(%#v) = %t, %v; want changed", timestamp, changed, err)
		}
	}
	if got := antichain.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2 incomparable timestamps", got)
	}
	if changed, err := antichain.Insert(mz006Timestamp{3, 3}); err != nil || !changed {
		t.Fatalf("middle Insert() = %t, %v; want changed", changed, err)
	}
	if got := antichain.Len(); got != 3 {
		t.Fatalf("Len() after middle timestamp = %d, want 3", got)
	}
}

func TestMZ006AntichainSnapshotIsIndependentAndClearResets(t *testing.T) {
	antichain, err := NewAntichain(mz006TimestampLessEqual)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := antichain.Insert(mz006Timestamp{1, 1}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := antichain.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot[0] = mz006Timestamp{99, 99}
	again, err := antichain.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if again[0] != (mz006Timestamp{1, 1}) {
		t.Fatalf("Snapshot() exposed internal storage: %#v", again)
	}
	antichain.Clear()
	if got := antichain.Len(); got != 0 {
		t.Fatalf("Len() after Clear() = %d, want 0", got)
	}
}

func TestMZ006AntichainValidatesNilAndConcurrentAccess(t *testing.T) {
	if _, err := NewAntichain[mz006Timestamp](nil); !errors.Is(err, ErrAntichainComparatorRequired) {
		t.Fatalf("nil comparator error = %v, want %v", err, ErrAntichainComparatorRequired)
	}
	var nilAntichain *Antichain[mz006Timestamp]
	if _, err := nilAntichain.Insert(mz006Timestamp{}); !errors.Is(err, ErrAntichainNil) {
		t.Fatalf("nil Insert() error = %v, want %v", err, ErrAntichainNil)
	}
	if _, err := nilAntichain.Covers(mz006Timestamp{}); !errors.Is(err, ErrAntichainNil) {
		t.Fatalf("nil Covers() error = %v, want %v", err, ErrAntichainNil)
	}
	if _, err := nilAntichain.Snapshot(); !errors.Is(err, ErrAntichainNil) {
		t.Fatalf("nil Snapshot() error = %v, want %v", err, ErrAntichainNil)
	}

	antichain, err := NewAntichain(mz006TimestampLessEqual)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for round := 0; round < 200; round++ {
				_, _ = antichain.Insert(mz006Timestamp{uint64(round), uint64(worker)})
				_, _ = antichain.Covers(mz006Timestamp{uint64(round + 1), uint64(worker + 1)})
			}
		}(worker)
	}
	group.Wait()
	if antichain.Len() == 0 {
		t.Fatal("concurrent antichain lost every timestamp")
	}
}
