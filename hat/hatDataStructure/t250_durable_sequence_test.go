package hatDataStructure

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func TestT250DurableSequencePersistsBeforePublishing(t *testing.T) {
	var persisted []uint64
	sequence, err := NewDurableSequence(0, func(value uint64) error {
		persisted = append(persisted, value)
		return nil
	})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	for want, expected := range []uint64{1, 2, 3} {
		got, err := sequence.Next()
		if err != nil || got != expected {
			t.Fatalf("Next() = %d, %v, want %d", got, err, expected)
		}
		if sequence.Current() != expected {
			t.Fatalf("Current() = %d, want %d", sequence.Current(), expected)
		}
		if persisted[want] != expected {
			t.Fatalf("persisted[%d] = %d, want %d", want, persisted[want], expected)
		}
	}
}

func TestT250DurableSequenceFailureDoesNotConsumeValue(t *testing.T) {
	var calls atomic.Int32
	sequence, err := NewDurableSequence(10, func(value uint64) error {
		if calls.Add(1) == 1 {
			return errors.New("disk unavailable")
		}
		if value != 11 {
			t.Fatalf("persisted value = %d, want 11", value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	if got, err := sequence.Next(); err == nil || got != 0 {
		t.Fatalf("failed Next() = %d, %v, want zero and error", got, err)
	}
	if got := sequence.Current(); got != 10 {
		t.Fatalf("Current() after failed persistence = %d, want 10", got)
	}
	if got, err := sequence.Next(); err != nil || got != 11 {
		t.Fatalf("retry Next() = %d, %v, want 11", got, err)
	}
}

func TestT250DurableSequenceRejectsOverflowAndNilConfiguration(t *testing.T) {
	if _, err := NewDurableSequence(0, nil); !errors.Is(err, ErrDurableSequencePersistRequired) {
		t.Fatalf("nil persist error = %v, want %v", err, ErrDurableSequencePersistRequired)
	}
	sequence, err := NewDurableSequence(math.MaxUint64, func(uint64) error {
		t.Fatal("overflow called persistence callback")
		return nil
	})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	if got, err := sequence.Next(); !errors.Is(err, ErrDurableSequenceOverflow) || got != 0 {
		t.Fatalf("overflow Next() = %d, %v", got, err)
	}
}

func TestT250DurableSequenceConcurrentCallsRemainGapSafe(t *testing.T) {
	sequence, err := NewDurableSequence(0, func(uint64) error { return nil })
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	const calls = 64
	values := make(chan uint64, calls)
	var group sync.WaitGroup
	group.Add(calls)
	for index := 0; index < calls; index++ {
		go func() {
			defer group.Done()
			value, err := sequence.Next()
			if err != nil {
				t.Errorf("Next() error = %v", err)
				return
			}
			values <- value
		}()
	}
	group.Wait()
	close(values)
	seen := make(map[uint64]struct{}, calls)
	for value := range values {
		if _, found := seen[value]; found {
			t.Fatalf("duplicate sequence value %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != calls || sequence.Current() != calls {
		t.Fatalf("values=%d Current()=%d, want %d", len(seen), sequence.Current(), calls)
	}
}
