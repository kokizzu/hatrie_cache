package hatSql

import (
	"errors"
	"strconv"
	"testing"
)

func TestGlobalJoinBroadcastPlannerPlansOnceAndCachesByEpoch(t *testing.T) {
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{
		Workers:           4,
		MaxBroadcastRows:  100,
		MaxBroadcastBytes: 1 << 20,
		MaxCachedPlans:    2,
	})
	if err != nil {
		t.Fatalf("NewGlobalJoinBroadcastPlanner() error = %v", err)
	}
	request := GlobalJoinBroadcastRequest{Fingerprint: "global:customers", Epoch: 7, Rows: 10, Bytes: 100}
	first, err := planner.Plan(request)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}
	if first.Mode != GlobalJoinBroadcastModeBroadcast || first.CacheHit || first.RemoteSubqueryExecutions != 1 || first.WorkerCopies != 4 || first.BroadcastBytes != 400 {
		t.Fatalf("first plan = %+v, want one fetch and four-worker broadcast", first)
	}
	second, err := planner.Plan(request)
	if err != nil {
		t.Fatalf("cached Plan() error = %v", err)
	}
	if second != (GlobalJoinBroadcastPlan{
		Mode:                     GlobalJoinBroadcastModeBroadcast,
		CacheHit:                 true,
		RemoteSubqueryExecutions: 0,
		WorkerCopies:             4,
		Rows:                     10,
		Bytes:                    100,
		BroadcastBytes:           400,
	}) {
		t.Fatalf("cached plan = %+v, want cache hit with no remote fetch", second)
	}

	nextEpoch, err := planner.Plan(GlobalJoinBroadcastRequest{Fingerprint: request.Fingerprint, Epoch: 8, Rows: 10, Bytes: 100})
	if err != nil {
		t.Fatalf("next epoch Plan() error = %v", err)
	}
	if nextEpoch.CacheHit || nextEpoch.RemoteSubqueryExecutions != 1 {
		t.Fatalf("next epoch plan = %+v, want fresh fetch", nextEpoch)
	}
	if got := planner.CachedPlans(); got != 2 {
		t.Fatalf("CachedPlans() = %d, want bounded cache size 2", got)
	}
	if removed := planner.Invalidate(request.Fingerprint); removed != 2 {
		t.Fatalf("Invalidate() removed %d plans, want 2", removed)
	}
	if got := planner.CachedPlans(); got != 0 {
		t.Fatalf("CachedPlans() after invalidate = %d, want zero", got)
	}
}

func TestGlobalJoinBroadcastPlannerFallsBackWhenPayloadIsLarge(t *testing.T) {
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 3, MaxBroadcastRows: 2, MaxBroadcastBytes: 8})
	if err != nil {
		t.Fatalf("NewGlobalJoinBroadcastPlanner() error = %v", err)
	}
	for name, request := range map[string]GlobalJoinBroadcastRequest{
		"rows":  {Fingerprint: "rows", Epoch: 1, Rows: 3, Bytes: 1},
		"bytes": {Fingerprint: "bytes", Epoch: 1, Rows: 1, Bytes: 9},
	} {
		plan, err := planner.Plan(request)
		if err != nil {
			t.Fatalf("%s Plan() error = %v", name, err)
		}
		if plan.Mode != GlobalJoinBroadcastModePerWorker || plan.CacheHit || plan.RemoteSubqueryExecutions != 3 || plan.WorkerCopies != 0 || plan.BroadcastBytes != 0 {
			t.Errorf("%s plan = %+v, want per-worker fallback", name, plan)
		}
	}
	if got := planner.CachedPlans(); got != 0 {
		t.Fatalf("fallback plans cached = %d, want zero", got)
	}
}

func TestGlobalJoinBroadcastPlannerValidatesBounds(t *testing.T) {
	if _, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{}); !errors.Is(err, ErrGlobalJoinBroadcastPlannerWorkersInvalid) {
		t.Fatalf("zero workers error = %v, want workers invalid", err)
	}
	if _, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 1, MaxBroadcastRows: -1}); !errors.Is(err, ErrGlobalJoinBroadcastPlannerOptionsInvalid) {
		t.Fatalf("negative rows error = %v, want options invalid", err)
	}
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 1})
	if err != nil {
		t.Fatalf("default planner error = %v", err)
	}
	for name, request := range map[string]GlobalJoinBroadcastRequest{
		"fingerprint": {Epoch: 1, Rows: 1, Bytes: 1},
		"rows":        {Fingerprint: "rows", Epoch: 1, Rows: -1, Bytes: 1},
		"bytes":       {Fingerprint: "bytes", Epoch: 1, Rows: 1, Bytes: -1},
	} {
		if _, err := planner.Plan(request); !errors.Is(err, ErrGlobalJoinBroadcastPlannerRequestInvalid) {
			t.Errorf("%s request error = %v, want request invalid", name, err)
		}
	}
}

func TestGlobalJoinBroadcastPlannerBoundsInvalidateQueue(t *testing.T) {
	planner, err := NewGlobalJoinBroadcastPlanner(GlobalJoinBroadcastPlannerOptions{Workers: 2, MaxCachedPlans: 1})
	if err != nil {
		t.Fatalf("NewGlobalJoinBroadcastPlanner() error = %v", err)
	}
	for index := 0; index < 4096; index++ {
		fingerprint := "global:" + strconv.Itoa(index)
		if _, err := planner.Plan(GlobalJoinBroadcastRequest{Fingerprint: fingerprint, Epoch: 1, Rows: 1, Bytes: 1}); err != nil {
			t.Fatalf("Plan(%d) error = %v", index, err)
		}
		if removed := planner.Invalidate(fingerprint); removed != 1 {
			t.Fatalf("Invalidate(%d) removed %d plans, want 1", index, removed)
		}
	}
	if got := planner.CachedPlans(); got != 0 {
		t.Fatalf("CachedPlans() = %d, want zero", got)
	}
	if got := len(planner.order); got > 1024 {
		t.Fatalf("stale cache order entries = %d, want bounded queue", got)
	}
}
