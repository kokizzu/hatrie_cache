package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type sqlSourceFrontierBarrierResult struct {
	frontier uint64
	err      error
}

func newTestSQLSourceFrontierBarrier(t *testing.T, count int) *SQLSourceFrontierBarrier {
	t.Helper()
	partitions := make([]SQLSourceFrontierPartition, count)
	for i := range partitions {
		partitions[i] = SQLSourceFrontierPartition{
			Source:    "source",
			Partition: string(rune('a' + i)),
		}
	}
	barrier, err := NewSQLSourceFrontierBarrierFromPartitions(partitions)
	if err != nil {
		t.Fatal(err)
	}
	return barrier
}

func TestSQLSourceFrontierBarrierWaitsForAllPartitions(t *testing.T) {
	barrier := newTestSQLSourceFrontierBarrier(t, 2)
	result := make(chan sqlSourceFrontierBarrierResult, 1)
	go func() {
		frontier, err := barrier.WaitForFrontier(context.Background(), 10)
		result <- sqlSourceFrontierBarrierResult{frontier: frontier, err: err}
	}()

	select {
	case got := <-result:
		t.Fatalf("wait returned before all partitions were observed: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	changed, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first observation was not reported as changed")
	}
	select {
	case got := <-result:
		t.Fatalf("wait returned with one partition missing: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	changed, err = barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "b", Frontier: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("second observation was not reported as changed")
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.frontier != 10 {
			t.Fatalf("frontier = %d, want 10", got.frontier)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not wake after every partition reached the frontier")
	}
}

func TestSQLSourceFrontierBarrierBatchWakeAndAtomicValidation(t *testing.T) {
	barrier := newTestSQLSourceFrontierBarrier(t, 3)
	changed, err := barrier.ObserveBatch([]SQLSourceFrontier{
		{Source: "source", Partition: "a", Frontier: 7},
		{Source: "source", Partition: "b", Frontier: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("changed = %d, want 2", changed)
	}

	result := make(chan sqlSourceFrontierBarrierResult, 1)
	go func() {
		frontier, err := barrier.WaitForFrontier(context.Background(), 7)
		result <- sqlSourceFrontierBarrierResult{frontier: frontier, err: err}
	}()
	select {
	case got := <-result:
		t.Fatalf("wait returned before batch completed the frontier: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	before := barrier.Snapshot()
	changed, err = barrier.ObserveBatch([]SQLSourceFrontier{
		{Source: "source", Partition: "c", Frontier: 7},
		{Source: "missing", Partition: "0", Frontier: 7},
	})
	if err == nil {
		t.Fatal("invalid batch returned nil error")
	}
	if changed != 0 {
		t.Fatalf("changed = %d after rejected batch, want 0", changed)
	}
	if after := barrier.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected batch changed state: before=%v after=%v", before, after)
	}
	select {
	case got := <-result:
		t.Fatalf("wait woke after rejected batch: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	changed, err = barrier.ObserveBatch([]SQLSourceFrontier{
		{Source: "source", Partition: "c", Frontier: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.frontier != 7 {
			t.Fatalf("frontier = %d, want 7", got.frontier)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not wake after the final partition was observed")
	}
}

func TestSQLSourceFrontierBarrierCancellation(t *testing.T) {
	barrier := newTestSQLSourceFrontierBarrier(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := barrier.WaitForFrontier(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled wait error = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := barrier.WaitForFrontier(ctx, 1)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not observe context cancellation")
	}
}

func TestSQLSourceFrontierBarrierDelegatesAndRejectsNil(t *testing.T) {
	barrier := newTestSQLSourceFrontierBarrier(t, 1)
	changed, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !changed || !barrier.ReadyAt(5) {
		t.Fatal("barrier did not delegate readiness")
	}
	if frontier, ok := barrier.Frontier("source", "a"); !ok || frontier != 5 {
		t.Fatalf("frontier = %d, %t, want 5, true", frontier, ok)
	}
	if frontier, ready := barrier.CommonFrontier(); !ready || frontier != 5 {
		t.Fatalf("common frontier = %d, %t, want 5, true", frontier, ready)
	}
	snapshot := barrier.Snapshot()
	if len(snapshot) != 1 || !snapshot[0].Observed || snapshot[0].Frontier != 5 {
		t.Fatalf("snapshot = %+v, want one observed frontier at 5", snapshot)
	}

	if _, err := NewSQLSourceFrontierBarrier(nil); !errors.Is(err, ErrSQLSourceFrontierBarrierTrackerNil) {
		t.Fatalf("nil tracker error = %v, want ErrSQLSourceFrontierBarrierTrackerNil", err)
	}
	var nilBarrier *SQLSourceFrontierBarrier
	if _, err := nilBarrier.WaitForFrontier(context.Background(), 0); !errors.Is(err, ErrSQLSourceFrontierBarrierNil) {
		t.Fatalf("nil barrier wait error = %v, want ErrSQLSourceFrontierBarrierNil", err)
	}
}
