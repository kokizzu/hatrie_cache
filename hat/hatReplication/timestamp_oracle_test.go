package hatReplication

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestTimestampOracleAdvancesAndObserves(t *testing.T) {
	oracle, err := NewTimestampOracle(10)
	if err != nil {
		t.Fatal(err)
	}
	if oracle.Current() != 10 {
		t.Fatalf("initial Current() = %d, want 10", oracle.Current())
	}
	if timestamp, err := oracle.Next(); err != nil || timestamp != 11 {
		t.Fatalf("first Next() = %d, %v, want 11", timestamp, err)
	}
	if err := oracle.Observe(7); err != nil {
		t.Fatalf("Observe(7) error = %v", err)
	}
	if oracle.Current() != 11 {
		t.Fatalf("Current() after older Observe = %d, want 11", oracle.Current())
	}
	if err := oracle.Observe(20); err != nil {
		t.Fatalf("Observe(20) error = %v", err)
	}
	if timestamp, err := oracle.Next(); err != nil || timestamp != 21 {
		t.Fatalf("Next() after Observe(20) = %d, %v, want 21", timestamp, err)
	}
}

func TestTimestampOracleRejectsInvalidAndOverflow(t *testing.T) {
	if _, err := NewTimestampOracle(-1); !errors.Is(err, ErrTimestampOracleInvalid) {
		t.Fatalf("negative initial error = %v, want ErrTimestampOracleInvalid", err)
	}
	var nilOracle *TimestampOracle
	if _, err := nilOracle.Next(); !errors.Is(err, ErrTimestampOracleInvalid) {
		t.Fatalf("nil Next() error = %v, want ErrTimestampOracleInvalid", err)
	}
	if err := nilOracle.Observe(1); !errors.Is(err, ErrTimestampOracleInvalid) {
		t.Fatalf("nil Observe() error = %v, want ErrTimestampOracleInvalid", err)
	}
	oracle, err := NewTimestampOracle(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Observe(-1); !errors.Is(err, ErrTimestampOracleInvalid) {
		t.Fatalf("negative Observe() error = %v, want ErrTimestampOracleInvalid", err)
	}
	if err := oracle.Observe(timestampOracleMax); err != nil {
		t.Fatalf("maximum Observe() error = %v", err)
	}
	if _, err := oracle.Next(); !errors.Is(err, ErrTimestampOracleOverflow) {
		t.Fatalf("overflow Next() error = %v, want ErrTimestampOracleOverflow", err)
	}
}

func TestTimestampOracleConcurrentNextIsUniqueAndMonotone(t *testing.T) {
	oracle, err := NewTimestampOracle(100)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const perWorker = 64
	values := make(chan int64, workers*perWorker)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for index := 0; index < perWorker; index++ {
				value, err := oracle.Next()
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				values <- value
			}
		}()
	}
	group.Wait()
	close(values)
	got := make([]int64, 0, workers*perWorker)
	for value := range values {
		got = append(got, value)
	}
	sort.Slice(got, func(left, right int) bool { return got[left] < got[right] })
	if len(got) != workers*perWorker {
		t.Fatalf("generated %d timestamps, want %d", len(got), workers*perWorker)
	}
	for index, value := range got {
		want := int64(101 + index)
		if value != want {
			t.Fatalf("timestamp %d = %d, want %d", index, value, want)
		}
	}
}

func BenchmarkTimestampOracleNext(b *testing.B) {
	oracle, err := NewTimestampOracle(0)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := oracle.Next(); err != nil {
			b.Fatal(err)
		}
	}
}
