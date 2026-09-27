package hatReplication

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestGlobalTimestampLeasePoolBatchesReservations(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var requests []GlobalTimestampRequest
	var requestsMu sync.Mutex
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		BatchSize: 4,
		Reserve: func(request GlobalTimestampRequest) (GlobalTimestampGrant, error) {
			requestsMu.Lock()
			requests = append(requests, request)
			requestsMu.Unlock()
			return oracle.Reserve(request)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for want := int64(1); want <= 10; want++ {
		got, err := pool.Next()
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if got != want {
			t.Fatalf("Next() = %d, want %d", got, want)
		}
	}

	stats := pool.Stats()
	if stats.Reservations != 3 {
		t.Fatalf("reservations = %d, want 3", stats.Reservations)
	}
	if stats.Issued != 10 {
		t.Fatalf("issued = %d, want 10", stats.Issued)
	}
	if stats.Remaining != 2 {
		t.Fatalf("remaining = %d, want 2", stats.Remaining)
	}
	if len(requests) != 3 {
		t.Fatalf("reserve calls = %d, want 3", len(requests))
	}
	for index, request := range requests {
		if request.Sequence != uint64(index+1) || request.Count != 4 {
			t.Fatalf("request[%d] = %#v, want sequence %d count 4", index, request, index+1)
		}
	}
}

func TestGlobalTimestampLeasePoolObserveRetiresUnusedRange(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		BatchSize: 4,
		Reserve:   oracle.Reserve,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := pool.Next(); err != nil || got != 1 {
		t.Fatalf("first Next() = (%d, %v), want (1, nil)", got, err)
	}
	if err := pool.Observe(100); err != nil {
		t.Fatal(err)
	}
	if got, err := pool.Next(); err != nil || got != 101 {
		t.Fatalf("observed Next() = (%d, %v), want (101, nil)", got, err)
	}
	stats := pool.Stats()
	if stats.Discarded != 3 {
		t.Fatalf("discarded = %d, want 3", stats.Discarded)
	}
}

func TestGlobalTimestampLeasePoolRetriesSameRequest(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var requests []GlobalTimestampRequest
	var attempts int
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		BatchSize: 2,
		Reserve: func(request GlobalTimestampRequest) (GlobalTimestampGrant, error) {
			requests = append(requests, request)
			attempts++
			grant, reserveErr := oracle.Reserve(request)
			if attempts == 1 {
				return grant, errors.New("simulated response loss")
			}
			return grant, reserveErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Next(); err == nil {
		t.Fatal("first Next() unexpectedly succeeded")
	}
	got, err := pool.Next()
	if err != nil {
		t.Fatalf("retry Next() error = %v", err)
	}
	if got != 1 {
		t.Fatalf("retry Next() = %d, want 1", got)
	}
	if len(requests) != 2 || requests[0].Sequence != 1 || requests[1].Sequence != 1 {
		t.Fatalf("retry requests = %#v, want two sequence-1 requests", requests)
	}
}

func TestGlobalTimestampLeasePoolConcurrentNextIsUnique(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		BatchSize: 16,
		Reserve:   oracle.Reserve,
	})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const perWorker = 50
	values := make([]int64, 0, workers*perWorker)
	var valuesMu sync.Mutex
	var workersWG sync.WaitGroup
	workersWG.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer workersWG.Done()
			local := make([]int64, 0, perWorker)
			for index := 0; index < perWorker; index++ {
				value, err := pool.Next()
				if err != nil {
					t.Errorf("Next() error = %v", err)
					return
				}
				local = append(local, value)
			}
			valuesMu.Lock()
			values = append(values, local...)
			valuesMu.Unlock()
		}()
	}
	workersWG.Wait()
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	if len(values) != workers*perWorker {
		t.Fatalf("value count = %d, want %d", len(values), workers*perWorker)
	}
	for index, value := range values {
		if value != int64(index+1) {
			t.Fatalf("values[%d] = %d, want %d", index, value, index+1)
		}
	}
}

func TestGlobalTimestampLeasePoolRejectsInvalidConfiguration(t *testing.T) {
	_, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{})
	if !errors.Is(err, ErrGlobalTimestampLeasePoolInvalid) {
		t.Fatalf("invalid options error = %v, want ErrGlobalTimestampLeasePoolInvalid", err)
	}
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		Reserve: func(GlobalTimestampRequest) (GlobalTimestampGrant, error) {
			return GlobalTimestampGrant{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Observe(-1); !errors.Is(err, ErrGlobalTimestampLeasePoolInvalid) {
		t.Fatalf("negative Observe() error = %v, want ErrGlobalTimestampLeasePoolInvalid", err)
	}
}
