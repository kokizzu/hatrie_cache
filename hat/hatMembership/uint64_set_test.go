package hatMembership

import (
	"reflect"
	"sync"
	"testing"
)

func TestUint64SetUsesSortedVectorForSmallInputs(t *testing.T) {
	values := []uint64{9, 2, 9, 4, 1}
	original := append([]uint64(nil), values...)
	set := BuildUint64(values)
	if set.Mode() != ModeSorted || set.Len() != 4 {
		t.Fatalf("mode/length = %v/%d, want sorted/4", set.Mode(), set.Len())
	}
	if !reflect.DeepEqual(values, original) {
		t.Fatal("BuildUint64 mutated its input")
	}
	for _, value := range []uint64{1, 2, 4, 9} {
		if !set.Contains(value) {
			t.Fatalf("Contains(%d) = false, want true", value)
		}
	}
	if set.Contains(3) || set.Contains(10) {
		t.Fatal("Contains returned true for an absent value")
	}
}

func TestUint64SetUsesDenseBitmap(t *testing.T) {
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i * 2)
	}
	set := BuildUint64(values)
	if set.Mode() != ModeBitmap {
		t.Fatalf("Mode = %v, want bitmap", set.Mode())
	}
	if !set.Contains(0) || !set.Contains(1998) || set.Contains(1999) {
		t.Fatal("bitmap membership result is incorrect")
	}
	if got := set.MemoryBytes(); got >= uint64(len(values))*16 {
		t.Fatalf("bitmap memory = %d, want less than hash estimate", got)
	}
}

func TestUint64SetUsesHashForLargeSparseInputs(t *testing.T) {
	values := make([]uint64, 512)
	for i := range values {
		values[i] = uint64(i) * 1_000_000_000
	}
	set := BuildUint64(values)
	if set.Mode() != ModeHash {
		t.Fatalf("Mode = %v, want hash", set.Mode())
	}
	if !set.Contains(values[511]) || set.Contains(values[511]+1) {
		t.Fatal("hash membership result is incorrect")
	}
}

func TestUint64SetHandlesEmptyAndMaxValues(t *testing.T) {
	if set := BuildUint64(nil); set.Len() != 0 || set.Contains(0) || set.Mode() != ModeEmpty {
		t.Fatalf("empty set = mode %v, length %d", set.Mode(), set.Len())
	}
	set := BuildUint64([]uint64{0, ^uint64(0)})
	if set.Contains(1) || !set.Contains(0) || !set.Contains(^uint64(0)) {
		t.Fatal("max-value membership result is incorrect")
	}
}

func TestUint64SetIsSafeForConcurrentReads(t *testing.T) {
	set := BuildUint64([]uint64{1, 2, 3, 4})
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := uint64(0); j < 10_000; j++ {
				want := j >= 1 && j <= 4
				if set.Contains(j) != want {
					t.Errorf("Contains(%d) = %v, want %v", j, set.Contains(j), want)
					return
				}
			}
		}()
	}
	group.Wait()
}
