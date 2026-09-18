//go:build !m033baseline

package hatSql

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestM033LogicalTimestampOracleReservesMonotoneRanges(t *testing.T) {
	oracle := NewSQLLogicalTimestampOracle(100)
	if got := oracle.Current(); got != 100 {
		t.Fatalf("Current() = %d, want 100", got)
	}
	first, err := oracle.Reserve(3)
	if err != nil {
		t.Fatalf("Reserve(3) error = %v", err)
	}
	if first.Start != 101 || first.End != 103 {
		t.Fatalf("first range = %#v, want 101..103", first)
	}
	if got := first.Count(); got != 3 {
		t.Fatalf("first.Count() = %d, want 3", got)
	}
	if got, err := oracle.Next(); err != nil || got != 104 {
		t.Fatalf("Next() = %d/%v, want 104/nil", got, err)
	}
	if !oracle.Observe(200) {
		t.Fatal("Observe(200) = false, want advancement")
	}
	if oracle.Observe(199) {
		t.Fatal("Observe(199) = true, want stale observation ignored")
	}
	second, err := oracle.Reserve(2)
	if err != nil {
		t.Fatalf("Reserve(2) error = %v", err)
	}
	if second.Start != 201 || second.End != 202 {
		t.Fatalf("second range = %#v, want 201..202", second)
	}
	if got := oracle.Current(); got != 202 {
		t.Fatalf("Current() = %d, want 202", got)
	}
}

func TestM033LogicalTimestampOracleValidatesNilCountAndOverflow(t *testing.T) {
	var nilOracle *SQLLogicalTimestampOracle
	if _, err := nilOracle.Next(); !errors.Is(err, ErrSQLLogicalTimestampOracleNil) {
		t.Fatalf("nil Next() error = %v, want %v", err, ErrSQLLogicalTimestampOracleNil)
	}
	oracle := NewSQLLogicalTimestampOracle(0)
	for _, count := range []int{0, -1} {
		if _, err := oracle.Reserve(count); !errors.Is(err, ErrSQLLogicalTimestampOracleCountInvalid) {
			t.Fatalf("Reserve(%d) error = %v, want %v", count, err, ErrSQLLogicalTimestampOracleCountInvalid)
		}
	}
	max := NewSQLLogicalTimestampOracle(^uint64(0))
	if _, err := max.Next(); !errors.Is(err, ErrSQLLogicalTimestampOracleExhausted) {
		t.Fatalf("max Next() error = %v, want %v", err, ErrSQLLogicalTimestampOracleExhausted)
	}
	if got := max.Current(); got != ^uint64(0) {
		t.Fatalf("max Current() = %d, want unchanged max", got)
	}
}

func TestM033LogicalTimestampOracleConcurrentRangesDoNotOverlap(t *testing.T) {
	const (
		workers         = 8
		rangesPerWorker = 32
		rangeSize       = 16
	)
	oracle := NewSQLLogicalTimestampOracle(0)
	ranges := make(chan SQLLogicalTimestampRange, workers*rangesPerWorker)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wait.Done()
			for index := 0; index < rangesPerWorker; index++ {
				reserved, err := oracle.Reserve(rangeSize)
				if err != nil {
					t.Errorf("Reserve(%d) error = %v", rangeSize, err)
					return
				}
				ranges <- reserved
			}
		}()
	}
	wait.Wait()
	close(ranges)

	ordered := make([]SQLLogicalTimestampRange, 0, cap(ranges))
	for reserved := range ranges {
		ordered = append(ordered, reserved)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].Start < ordered[right].Start
	})
	for index, reserved := range ordered {
		if reserved.End-reserved.Start+1 != rangeSize {
			t.Fatalf("range %d = %#v, want size %d", index, reserved, rangeSize)
		}
		if index > 0 && reserved.Start != ordered[index-1].End+1 {
			t.Fatalf("range %d starts at %d after %d, want contiguous reservation", index, reserved.Start, ordered[index-1].End)
		}
	}
}
