package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestM250TemporalJoinAlignmentRejectsInvalidInputs(t *testing.T) {
	if _, err := NewSQLTemporalJoinFrontierAlignment(nil, nil); !errors.Is(err, ErrSQLTemporalJoinFrontierAlignmentLeftNil) {
		t.Fatalf("nil left error = %v, want left nil error", err)
	}
	left, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "left", Partition: "0"}})
	if err != nil {
		t.Fatalf("left barrier: %v", err)
	}
	if _, err := NewSQLTemporalJoinFrontierAlignment(left, nil); !errors.Is(err, ErrSQLTemporalJoinFrontierAlignmentRightNil) {
		t.Fatalf("nil right error = %v, want right nil error", err)
	}
	var alignment *SQLTemporalJoinFrontierAlignment
	if _, err := alignment.WaitForFrontier(context.Background(), 1); !errors.Is(err, ErrSQLTemporalJoinFrontierAlignmentNil) {
		t.Fatalf("nil alignment error = %v, want alignment nil error", err)
	}
}

func TestM250TemporalJoinAlignmentUsesReadyFastPath(t *testing.T) {
	left, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "left", Partition: "0"}})
	if err != nil {
		t.Fatalf("left barrier: %v", err)
	}
	right, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "right", Partition: "0"}})
	if err != nil {
		t.Fatalf("right barrier: %v", err)
	}
	if _, err := left.Observe(SQLSourceFrontier{Source: "left", Partition: "0", Frontier: 7}); err != nil {
		t.Fatalf("left observe: %v", err)
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "0", Frontier: 8}); err != nil {
		t.Fatalf("right observe: %v", err)
	}
	alignment, err := NewSQLTemporalJoinFrontierAlignment(left, right)
	if err != nil {
		t.Fatalf("alignment: %v", err)
	}
	result, err := alignment.WaitForFrontier(context.Background(), 7)
	if err != nil {
		t.Fatalf("ready alignment wait: %v", err)
	}
	if result.LeftFrontier != 7 || result.RightFrontier != 8 || result.CommonFrontier != 7 {
		t.Fatalf("ready alignment result = %+v, want left=7 right=8 common=7", result)
	}
	if !alignment.ReadyAt(7) || alignment.ReadyAt(8) {
		t.Fatalf("ReadyAt results = %v/%v, want true/false", alignment.ReadyAt(7), alignment.ReadyAt(8))
	}
}

func TestM250TemporalJoinAlignmentWaitsForBothInputFrontiers(t *testing.T) {
	left, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "left", Partition: "0"}})
	if err != nil {
		t.Fatalf("left barrier: %v", err)
	}
	right, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "right", Partition: "0"}})
	if err != nil {
		t.Fatalf("right barrier: %v", err)
	}
	alignment, err := NewSQLTemporalJoinFrontierAlignment(left, right)
	if err != nil {
		t.Fatalf("alignment: %v", err)
	}
	if _, err := left.Observe(SQLSourceFrontier{Source: "left", Partition: "0", Frontier: 9}); err != nil {
		t.Fatalf("left observe: %v", err)
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "0", Frontier: 4}); err != nil {
		t.Fatalf("right observe: %v", err)
	}

	resultCh := make(chan struct {
		result SQLTemporalJoinFrontierResult
		err    error
	}, 1)
	go func() {
		result, err := alignment.WaitForFrontier(context.Background(), 5)
		resultCh <- struct {
			result SQLTemporalJoinFrontierResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case result := <-resultCh:
		t.Fatalf("alignment returned before right input reached target: %+v", result)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "0", Frontier: 5}); err != nil {
		t.Fatalf("right catch-up observe: %v", err)
	}
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("alignment wait: %v", result.err)
		}
		if result.result.LeftFrontier != 9 || result.result.RightFrontier != 5 || result.result.CommonFrontier != 5 {
			t.Fatalf("alignment result = %+v, want left=9 right=5 common=5", result.result)
		}
	case <-time.After(time.Second):
		t.Fatal("alignment did not wake after both inputs reached target")
	}
}

func TestM250TemporalJoinAlignmentHonorsCancellation(t *testing.T) {
	left, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "left", Partition: "0"}})
	if err != nil {
		t.Fatalf("left barrier: %v", err)
	}
	right, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "right", Partition: "0"}})
	if err != nil {
		t.Fatalf("right barrier: %v", err)
	}
	alignment, err := NewSQLTemporalJoinFrontierAlignment(left, right)
	if err != nil {
		t.Fatalf("alignment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := alignment.WaitForFrontier(ctx, 1); err == nil {
		t.Fatal("alignment wait succeeded without either input frontier")
	}
}
