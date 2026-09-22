package hatReplication

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestChangefeedSnapshotBoundaryAtomicallyCommitsFirstLiveFrontier(t *testing.T) {
	boundary, err := NewChangefeedSnapshotBoundary("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.AdvanceLiveFrontier(1); !errors.Is(err, ErrChangefeedSnapshotBoundaryNotCommitted) {
		t.Fatalf("frontier should not advance before snapshot commit, got %v", err)
	}
	committed, err := boundary.CommitSnapshotBoundary(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := ChangefeedSnapshotBoundarySnapshot{
		Source:            "orders",
		SnapshotOffset:    100,
		FirstLiveFrontier: 100,
		LiveFrontier:      100,
		Committed:         true,
	}
	if !reflect.DeepEqual(committed, want) {
		t.Fatalf("unexpected committed boundary: want=%+v got=%+v", want, committed)
	}
	if retry, err := boundary.CommitSnapshotBoundary(100, 100); err != nil || !reflect.DeepEqual(retry, want) {
		t.Fatalf("identical commit should be idempotent: result=%+v err=%v", retry, err)
	}
	if _, err := boundary.CommitSnapshotBoundary(100, 101); !errors.Is(err, ErrChangefeedSnapshotBoundaryConflict) {
		t.Fatalf("different first frontier should conflict, got %v", err)
	}
	if _, err := boundary.AdvanceLiveFrontier(99); !errors.Is(err, ErrChangefeedSnapshotBoundaryRegressed) {
		t.Fatalf("frontier regression should be rejected, got %v", err)
	}
	advanced, err := boundary.AdvanceLiveFrontier(105)
	if err != nil {
		t.Fatal(err)
	}
	if advanced.LiveFrontier != 105 || advanced.FirstLiveFrontier != 100 || advanced.SnapshotOffset != 100 {
		t.Fatalf("advancing frontier changed the coupled boundary: %+v", advanced)
	}
}

func TestChangefeedSnapshotBoundaryRejectsInvalidCoupling(t *testing.T) {
	if _, err := NewChangefeedSnapshotBoundary(""); !errors.Is(err, ErrChangefeedSnapshotBoundaryInvalid) {
		t.Fatalf("empty source should be rejected, got %v", err)
	}
	boundary, err := NewChangefeedSnapshotBoundary("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.CommitSnapshotBoundary(101, 100); !errors.Is(err, ErrChangefeedSnapshotBoundaryInvalid) {
		t.Fatalf("first live frontier below snapshot offset should be rejected, got %v", err)
	}
	if _, err := boundary.CommitSnapshotBoundary(100, 100); err != nil {
		t.Fatal(err)
	}
	invalid := boundary.Snapshot()
	invalid.FirstLiveFrontier--
	if err := boundary.Restore(invalid); !errors.Is(err, ErrChangefeedSnapshotBoundaryInvalid) {
		t.Fatalf("invalid restore should be rejected, got %v", err)
	}
	if got := boundary.Snapshot(); got.FirstLiveFrontier != 100 || got.LiveFrontier != 100 {
		t.Fatalf("invalid restore changed coupled state: %+v", got)
	}
}

func TestChangefeedSnapshotBoundaryRoundTripAndMalformedData(t *testing.T) {
	boundary, err := NewChangefeedSnapshotBoundary("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.CommitSnapshotBoundary(100, 105); err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.AdvanceLiveFrontier(110); err != nil {
		t.Fatal(err)
	}
	want := boundary.Snapshot()
	encoded, err := boundary.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalChangefeedSnapshotBoundary(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("boundary changed across binary round trip: want=%+v got=%+v", want, decoded)
	}
	restored, err := NewChangefeedSnapshotBoundaryFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored boundary differs: want=%+v got=%+v", want, got)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("invalid"),
	} {
		if _, err := UnmarshalChangefeedSnapshotBoundary(malformed); !errors.Is(err, ErrChangefeedSnapshotBoundarySnapshotInvalid) {
			t.Errorf("expected malformed boundary error, got %v", err)
		}
	}
}

func TestChangefeedSnapshotBoundaryConcurrentCommitHasOneBoundary(t *testing.T) {
	boundary, err := NewChangefeedSnapshotBoundary("orders")
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 32
	var wg sync.WaitGroup
	wg.Add(contenders)
	committed := make(chan ChangefeedSnapshotBoundarySnapshot, contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wg.Done()
			state, err := boundary.CommitSnapshotBoundary(100, uint64(100+index))
			if err == nil {
				committed <- state
			} else if !errors.Is(err, ErrChangefeedSnapshotBoundaryConflict) {
				t.Errorf("unexpected concurrent commit error: %v", err)
			}
		}(index)
	}
	wg.Wait()
	close(committed)
	count := 0
	for range committed {
		count++
	}
	if count != 1 {
		t.Fatalf("expected one distinct boundary commit, got %d", count)
	}
}
