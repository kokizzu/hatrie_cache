package hatPipeline_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestMZ008FrontierAntichainKeepsOnlyMinimalPoints(t *testing.T) {
	antichain, err := hatPipeline.NewFrontierAntichain(2, hatPipeline.FrontierAntichainOptions{MaxPoints: 4})
	if err != nil {
		t.Fatalf("NewFrontierAntichain() error = %v", err)
	}

	for _, point := range [][]uint64{{5, 1}, {1, 5}, {3, 3}} {
		inserted, err := antichain.Insert(point)
		if err != nil || !inserted {
			t.Fatalf("Insert(%v) = %v, %v; want inserted", point, inserted, err)
		}
	}
	if inserted, err := antichain.Insert([]uint64{4, 4}); err != nil || inserted {
		t.Fatalf("Insert(dominated) = %v, %v; want false, nil", inserted, err)
	}
	if covered, err := antichain.Covers([]uint64{6, 6}); err != nil || !covered {
		t.Fatalf("Covers(covered) = %v, %v; want true, nil", covered, err)
	}
	if covered, err := antichain.Covers([]uint64{0, 0}); err != nil || covered {
		t.Fatalf("Covers(ahead) = %v, %v; want false, nil", covered, err)
	}
	if inserted, err := antichain.Insert([]uint64{0, 4}); err != nil || !inserted {
		t.Fatalf("Insert(new minimum) = %v, %v; want true, nil", inserted, err)
	}

	snapshot := antichain.Snapshot()
	if snapshot.Dimensions != 2 || snapshot.Len() != 3 {
		t.Fatalf("Snapshot() = %#v, want 3 two-dimensional points", snapshot)
	}
	got := make([][]uint64, 0, snapshot.Len())
	for index := 0; index < snapshot.Len(); index++ {
		point, ok := snapshot.At(index)
		if !ok {
			t.Fatalf("Snapshot.At(%d) returned false", index)
		}
		got = append(got, point)
	}
	want := [][]uint64{{5, 1}, {3, 3}, {0, 4}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("minimal points = %#v, want %#v", got, want)
	}

	got[0][0] = 999
	snapshot.Values[0] = 999
	refetched := antichain.Snapshot()
	point, _ := refetched.At(0)
	if point[0] == 999 {
		t.Fatal("Snapshot.At() exposed mutable antichain storage")
	}
}

func TestMZ008FrontierAntichainValidatesDimensionsAndLimit(t *testing.T) {
	if _, err := hatPipeline.NewFrontierAntichain(0, hatPipeline.FrontierAntichainOptions{}); !errors.Is(err, hatPipeline.ErrFrontierAntichainOptionsInvalid) {
		t.Fatalf("zero dimensions error = %v, want %v", err, hatPipeline.ErrFrontierAntichainOptionsInvalid)
	}
	if _, err := hatPipeline.NewFrontierAntichain(2, hatPipeline.FrontierAntichainOptions{MaxPoints: 1, InitialPoints: 2}); !errors.Is(err, hatPipeline.ErrFrontierAntichainOptionsInvalid) {
		t.Fatalf("initial points over limit error = %v, want %v", err, hatPipeline.ErrFrontierAntichainOptionsInvalid)
	}
	antichain, err := hatPipeline.NewFrontierAntichain(2, hatPipeline.FrontierAntichainOptions{MaxPoints: 1})
	if err != nil {
		t.Fatalf("NewFrontierAntichain(limit) error = %v", err)
	}
	if _, err := antichain.Insert([]uint64{1}); !errors.Is(err, hatPipeline.ErrFrontierAntichainPointInvalid) {
		t.Fatalf("wrong dimension error = %v, want %v", err, hatPipeline.ErrFrontierAntichainPointInvalid)
	}
	if _, err := antichain.Covers([]uint64{1}); !errors.Is(err, hatPipeline.ErrFrontierAntichainPointInvalid) {
		t.Fatalf("Covers(wrong dimension) error = %v, want %v", err, hatPipeline.ErrFrontierAntichainPointInvalid)
	}
	if _, err := antichain.Insert([]uint64{1, 1}); err != nil {
		t.Fatalf("Insert(first) error = %v", err)
	}
	if _, err := antichain.Insert([]uint64{0, 2}); !errors.Is(err, hatPipeline.ErrFrontierAntichainLimit) {
		t.Fatalf("incomparable over limit error = %v, want %v", err, hatPipeline.ErrFrontierAntichainLimit)
	}
	if antichain.Len() != 1 {
		t.Fatalf("Len() after rejected insert = %d, want 1", antichain.Len())
	}
}

func ExampleFrontierAntichain() {
	antichain, _ := hatPipeline.NewFrontierAntichain(2, hatPipeline.FrontierAntichainOptions{})
	_, _ = antichain.Insert([]uint64{5, 1})
	_, _ = antichain.Insert([]uint64{1, 5})
	covered, _ := antichain.Covers([]uint64{5, 5})
	fmt.Println(covered, antichain.Len())
	// Output: true 2
}
