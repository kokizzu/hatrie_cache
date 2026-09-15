package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
)

func TestFrontierReadHoldPinsCompactionAndAdvances(t *testing.T) {
	holds := NewFrontierReadHoldSet()
	if got := holds.SafeSince(10); got != 10 {
		t.Fatalf("SafeSince without holds = %d, want 10", got)
	}
	if !holds.CanCompactThrough(10) {
		t.Fatal("empty hold set should allow compaction")
	}

	first, err := holds.Acquire(3, 7)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if first.Since() != 3 || first.Upper() != 7 {
		t.Fatalf("first bounds = %d..%d, want 3..7", first.Since(), first.Upper())
	}
	if got := holds.SafeSince(10); got != 3 {
		t.Fatalf("SafeSince with first hold = %d, want 3", got)
	}
	if holds.CanCompactThrough(3) {
		t.Fatal("compaction through a held since frontier must be rejected")
	}
	if !holds.CanCompactThrough(2) {
		t.Fatal("compaction before a held since frontier should be allowed")
	}
	if !first.Allows(3) || !first.Allows(7) || first.Allows(2) || first.Allows(8) {
		t.Fatal("read hold bounds are not inclusive and stable")
	}

	second, err := holds.Acquire(5, 9)
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	if got := holds.SafeSince(10); got != 3 {
		t.Fatalf("SafeSince with two holds = %d, want 3", got)
	}
	if got := holds.Active(); got != 2 {
		t.Fatalf("Active() = %d, want 2", got)
	}
	if err := first.Advance(6, 11); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if got := holds.SafeSince(10); got != 5 {
		t.Fatalf("SafeSince after first advance = %d, want 5", got)
	}
	if first.Since() != 6 || first.Upper() != 11 || !first.Allows(10) {
		t.Fatalf("advanced first bounds = %d..%d", first.Since(), first.Upper())
	}
	if err := second.Release(); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("second Release() should be idempotent: %v", err)
	}
	if got := holds.Active(); got != 1 {
		t.Fatalf("Active() after second release = %d, want 1", got)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if got := holds.SafeSince(10); got != 10 {
		t.Fatalf("SafeSince after release = %d, want 10", got)
	}
	if got := holds.Active(); got != 0 {
		t.Fatalf("Active() after release = %d, want 0", got)
	}
}

func TestFrontierReadHoldRejectsInvalidTransitions(t *testing.T) {
	holds := NewFrontierReadHoldSet()
	if _, err := holds.Acquire(8, 7); !errors.Is(err, ErrFrontierReadHoldInvalidBounds) {
		t.Fatalf("invalid Acquire() error = %v", err)
	}
	hold, err := holds.Acquire(2, 4)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if err := hold.Advance(1, 4); !errors.Is(err, ErrFrontierReadHoldRegression) {
		t.Fatalf("regressing since Advance() error = %v", err)
	}
	if err := hold.Advance(2, 3); !errors.Is(err, ErrFrontierReadHoldRegression) {
		t.Fatalf("regressing upper Advance() error = %v", err)
	}
	if err := hold.Advance(4, 6); err != nil {
		t.Fatalf("valid Advance() error = %v", err)
	}
	if err := hold.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := hold.Advance(5, 7); !errors.Is(err, ErrFrontierReadHoldReleased) {
		t.Fatalf("released Advance() error = %v", err)
	}
	if hold.Allows(5) {
		t.Fatal("released hold must not allow reads")
	}
}

func TestFrontierReadHoldConcurrentAcquireAdvanceRelease(t *testing.T) {
	holds := NewFrontierReadHoldSet()
	const workers = 32
	const iterations = 100
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer group.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				since := uint64(worker + iteration)
				hold, err := holds.Acquire(since, since+1)
				if err != nil {
					t.Errorf("Acquire() error = %v", err)
					return
				}
				if err := hold.Advance(since+1, since+2); err != nil {
					t.Errorf("Advance() error = %v", err)
				}
				if err := hold.Release(); err != nil {
					t.Errorf("Release() error = %v", err)
				}
			}
		}(worker)
	}
	group.Wait()
	if got := holds.Active(); got != 0 {
		t.Fatalf("Active() after concurrent operations = %d, want 0", got)
	}
}
