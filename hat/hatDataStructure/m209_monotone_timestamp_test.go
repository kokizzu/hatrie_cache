package hatDataStructure_test

import (
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestM209MonotoneLogicalTimestampAdvancesAndRejectsRegression(t *testing.T) {
	timestamp := hatDataStructure.NewMonotoneLogicalTimestamp(10)
	if timestamp.Current() != 10 || !timestamp.AtLeast(10) || timestamp.AtLeast(11) {
		t.Fatalf("initial timestamp = %d, AtLeast(10/11) = %v/%v", timestamp.Current(), timestamp.AtLeast(10), timestamp.AtLeast(11))
	}
	if advanced, err := timestamp.Advance(10); advanced || err != nil {
		t.Fatalf("equal Advance() = %v/%v, want false/nil", advanced, err)
	}
	if advanced, err := timestamp.Advance(12); !advanced || err != nil {
		t.Fatalf("forward Advance() = %v/%v, want true/nil", advanced, err)
	}
	if advanced, err := timestamp.Advance(11); advanced || !errors.Is(err, hatDataStructure.ErrMonotoneLogicalTimestampRegressed) {
		t.Fatalf("regressed Advance() = %v/%v, want false/regression", advanced, err)
	}
	if timestamp.Current() != 12 {
		t.Fatalf("Current() after regression = %d, want 12", timestamp.Current())
	}
}

func TestM209MonotoneLogicalTimestampZeroAndNilReceivers(t *testing.T) {
	var zero hatDataStructure.MonotoneLogicalTimestamp
	if zero.Current() != 0 || !zero.AtLeast(0) {
		t.Fatalf("zero timestamp = %d, AtLeast(0) = %v", zero.Current(), zero.AtLeast(0))
	}
	if advanced, err := zero.Advance(3); !advanced || err != nil || zero.Current() != 3 {
		t.Fatalf("zero Advance() = %v/%v, current %d", advanced, err, zero.Current())
	}

	var nilTimestamp *hatDataStructure.MonotoneLogicalTimestamp
	if nilTimestamp.Current() != 0 || nilTimestamp.AtLeast(0) {
		t.Fatalf("nil timestamp Current/AtLeast = %d/%v", nilTimestamp.Current(), nilTimestamp.AtLeast(0))
	}
	if advanced, err := nilTimestamp.Advance(1); advanced || !errors.Is(err, hatDataStructure.ErrMonotoneLogicalTimestampNil) {
		t.Fatalf("nil Advance() = %v/%v, want false/nil error", advanced, err)
	}
}

func TestM209MonotoneLogicalTimestampConcurrentAdvanceKeepsMaximum(t *testing.T) {
	timestamp := hatDataStructure.NewMonotoneLogicalTimestamp(0)
	var wait sync.WaitGroup
	for value := uint64(1); value <= 64; value++ {
		value := value
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _ = timestamp.Advance(value)
		}()
	}
	wait.Wait()
	if got := timestamp.Current(); got != 64 {
		t.Fatalf("concurrent Current() = %d, want 64", got)
	}
}
