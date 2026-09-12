package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

var (
	benchmarkU64AntichainSink   *U64Antichain
	benchmarkNaiveAntichainSink [][]uint64
)

func TestU64AntichainMaintainsMinimalFrontier(t *testing.T) {
	antichain, err := NewU64Antichain(2)
	if err != nil {
		t.Fatalf("NewU64Antichain() error = %v", err)
	}
	for _, point := range [][]uint64{{2, 2}, {1, 3}} {
		changed, err := antichain.Add(point)
		if err != nil || !changed {
			t.Fatalf("Add(%v) = (%v, %v), want (true, nil)", point, changed, err)
		}
	}
	if changed, err := antichain.Add([]uint64{3, 3}); err != nil || changed {
		t.Fatalf("dominated Add() = (%v, %v), want (false, nil)", changed, err)
	}
	if changed, err := antichain.Add([]uint64{2, 2}); err != nil || changed {
		t.Fatalf("duplicate Add() = (%v, %v), want (false, nil)", changed, err)
	}

	got, err := antichain.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if want := []uint64{2, 2, 1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %v, want %v", got, want)
	}
	if got, want := antichain.Len(), 2; got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}

	for _, test := range []struct {
		point []uint64
		want  bool
	}{
		{point: []uint64{4, 4}, want: true},
		{point: []uint64{1, 2}, want: false},
		{point: []uint64{0, 4}, want: false},
	} {
		got, err := antichain.Covers(test.point)
		if err != nil {
			t.Fatalf("Covers(%v) error = %v", test.point, err)
		}
		if got != test.want {
			t.Fatalf("Covers(%v) = %v, want %v", test.point, got, test.want)
		}
	}

	changed, err := antichain.Add([]uint64{1, 2})
	if err != nil || !changed {
		t.Fatalf("dominating Add() = (%v, %v), want (true, nil)", changed, err)
	}
	got, err = antichain.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot() after dominance error = %v", err)
	}
	if want := []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() after dominance = %v, want %v", got, want)
	}
}

func TestU64AntichainValidationAndAtomicCapacity(t *testing.T) {
	antichain, err := NewU64AntichainWithOptions(U64AntichainOptions{Dimensions: 2, MaxEntries: 1, InitialEntries: 1})
	if err != nil {
		t.Fatalf("NewU64AntichainWithOptions() error = %v", err)
	}
	if changed, err := antichain.Add([]uint64{2, 2}); err != nil || !changed {
		t.Fatalf("initial Add() = (%v, %v), want (true, nil)", changed, err)
	}
	if changed, err := antichain.Add([]uint64{1, 3}); !errors.Is(err, ErrU64AntichainMaxEntries) || changed {
		t.Fatalf("capacity Add() = (%v, %v), want (false, max entries error)", changed, err)
	}
	if got, err := antichain.Snapshot(nil); err != nil || !reflect.DeepEqual(got, []uint64{2, 2}) {
		t.Fatalf("snapshot after rejected Add() = (%v, %v), want ([2 2], nil)", got, err)
	}
	if changed, err := antichain.Add([]uint64{1, 1}); err != nil || !changed {
		t.Fatalf("capacity-releasing Add() = (%v, %v), want (true, nil)", changed, err)
	}

	for _, point := range [][]uint64{{1}, {1, 2, 3}} {
		if changed, err := antichain.Add(point); !errors.Is(err, ErrU64AntichainPointDimensions) || changed {
			t.Fatalf("invalid Add(%v) = (%v, %v), want (false, dimension error)", point, changed, err)
		}
		if covered, err := antichain.Covers(point); !errors.Is(err, ErrU64AntichainPointDimensions) || covered {
			t.Fatalf("invalid Covers(%v) = (%v, %v), want (false, dimension error)", point, covered, err)
		}
	}
	if _, err := NewU64Antichain(0); !errors.Is(err, ErrU64AntichainDimensions) {
		t.Fatalf("zero dimensions error = %v, want %v", err, ErrU64AntichainDimensions)
	}
	if _, err := NewU64AntichainWithOptions(U64AntichainOptions{Dimensions: 2, MaxEntries: -1}); !errors.Is(err, ErrU64AntichainMaxEntries) {
		t.Fatalf("negative max entries error = %v, want %v", err, ErrU64AntichainMaxEntries)
	}
	if _, err := NewU64AntichainWithOptions(U64AntichainOptions{Dimensions: 2, MaxEntries: 1, InitialEntries: 2}); !errors.Is(err, ErrU64AntichainInitialEntries) {
		t.Fatalf("initial capacity over max error = %v, want %v", err, ErrU64AntichainInitialEntries)
	}
}

func TestU64AntichainSnapshotReuseResetAndNilReceiver(t *testing.T) {
	antichain, err := NewU64Antichain(2)
	if err != nil {
		t.Fatalf("NewU64Antichain() error = %v", err)
	}
	if _, err := antichain.Add([]uint64{4, 5}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	destination := make([]uint64, 0, 4)
	got, err := antichain.Snapshot(destination)
	if err != nil {
		t.Fatalf("Snapshot(destination) error = %v", err)
	}
	if len(got) != 2 || &got[:1][0] != &destination[:1][0] {
		t.Fatalf("Snapshot(destination) did not reuse destination: got=%v", got)
	}
	antichain.Reset()
	if got := antichain.Len(); got != 0 {
		t.Fatalf("Len() after Reset = %d, want 0", got)
	}
	if _, err := antichain.Add([]uint64{7, 8}); err != nil {
		t.Fatalf("Add() after Reset error = %v", err)
	}

	var nilAntichain *U64Antichain
	if _, err := nilAntichain.Add([]uint64{1, 2}); !errors.Is(err, ErrU64AntichainNil) {
		t.Fatalf("nil Add() error = %v, want %v", err, ErrU64AntichainNil)
	}
	if _, err := nilAntichain.Covers([]uint64{1, 2}); !errors.Is(err, ErrU64AntichainNil) {
		t.Fatalf("nil Covers() error = %v, want %v", err, ErrU64AntichainNil)
	}
	if _, err := nilAntichain.Snapshot(nil); !errors.Is(err, ErrU64AntichainNil) {
		t.Fatalf("nil Snapshot() error = %v, want %v", err, ErrU64AntichainNil)
	}
	if got := nilAntichain.Len(); got != 0 {
		t.Fatalf("nil Len() = %d, want 0", got)
	}
	nilAntichain.Reset()
}

func TestU64AntichainConcurrentUse(t *testing.T) {
	antichain, err := NewU64Antichain(2)
	if err != nil {
		t.Fatalf("NewU64Antichain() error = %v", err)
	}
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := uint64(0); iteration < 250; iteration++ {
				point := []uint64{iteration + uint64(worker), 1000 - iteration}
				if _, err := antichain.Add(point); err != nil {
					t.Errorf("Add(%v) error = %v", point, err)
					return
				}
				if _, err := antichain.Covers([]uint64{1000, 1000}); err != nil {
					t.Errorf("Covers() error = %v", err)
					return
				}
			}
		}()
	}
	waitGroup.Wait()
}

func BenchmarkU64AntichainBuild(b *testing.B) {
	const entries = 512
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		antichain, err := NewU64AntichainWithOptions(U64AntichainOptions{Dimensions: 2, InitialEntries: entries})
		if err != nil {
			b.Fatal(err)
		}
		for entry := uint64(0); entry < entries; entry++ {
			if _, err := antichain.Add([]uint64{entry, entries - entry}); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkU64AntichainSink = antichain
	}
}

func BenchmarkNaiveAntichainBuild(b *testing.B) {
	const entries = 512
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		points := make([][]uint64, 0, entries)
		for entry := uint64(0); entry < entries; entry++ {
			candidate := []uint64{entry, entries - entry}
			dominated := false
			for _, point := range points {
				if u64PointLessEqual(point, candidate) {
					dominated = true
					break
				}
			}
			if dominated {
				continue
			}
			retained := points[:0]
			for _, point := range points {
				if !u64PointLessEqual(candidate, point) {
					retained = append(retained, point)
				}
			}
			points = append(retained, candidate)
		}
		benchmarkNaiveAntichainSink = points
	}
}

func u64PointLessEqual(left, right []uint64) bool {
	for index, value := range left {
		if value > right[index] {
			return false
		}
	}
	return true
}
